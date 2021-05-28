"""
This provides a utility function for splitting large input files from Bucket
and piping them into a bash script for parallel execution.
"""

import logging
from itertools import islice
from math import ceil
from typing import Any, Dict, List, Optional, Tuple

from dataswarm.operators import (
    BashOperator,
    DynamicPipeOperator,
    BucketStore,
    PythonOperator,
)


BaseOperator = Any

BUCKET = "malware_anal_infra_dataflow"
BASEPATH = f"{BUCKET}/tree/tmp/<DATEID>"
JOINED_OUTPUT_FILENAME = "joined_result"
METADATA_FILENAME = "job_metadata"
INFILE_PREFIX = "input_part_"
OUTFILE_PREFIX = "output_part_"

# Note that DynamicPipelineOperator has a limit of 14K tasks, and chronos
# has a limit of 4000 subtasks.
MAX_TASK_COUNT = 4000
MIN_PARTITION_SIZE = 1000


def _get_num_files(num_files_path: str) -> int:
    metadata_file = _get_bucket_store(num_files_path)
    with metadata_file.open("r") as fh:
        contents = fh.read()

    # Assert contents to be int
    try:
        num_files = int(contents)
    except ValueError:
        logging.error(f"{num_files_path} contents {contents} isn't a valid int")
        raise
    return num_files


def _calc_partition_size(path: str) -> int:
    input_file = _get_bucket_store(path)
    cnt = 0

    logging.info("Calculating file length")
    with input_file.open("r") as fh:
        for _line in fh:
            cnt += 1

    if cnt == 0:
        return 0

    partition_size = max(ceil(cnt / MAX_TASK_COUNT), MIN_PARTITION_SIZE)
    logging.info(
        f"Estimated partion size of {partition_size} for file length {cnt} - "
        f"total of {ceil(cnt/partition_size)} tasks"
    )
    return partition_size


def _get_manifold_store(path: str) -> str:
    return BucketStore(
        path=path,
        retention_days=1,
        contains_pii=False,
    ).expand_macro(lambda x: x)


def split_input_func(
    input_file_path: str, output_dir: str, split_num_lines: Optional[int] = None
) -> None:
    if split_num_lines is None:
        split_num_lines = _calc_partition_size(input_file_path)

    if split_num_lines == 0:
        raise RuntimeError(f"Input file {input_file_path} is empty")

    logging.info(
        f"Splitting input file {input_file_path} into batches of size {split_num_lines}"
    )

    data_file = _get_bucket_store(input_file_path)
    n = 0
    with data_file.open("r") as input_file:
        next_n_lines = list(islice(input_file, split_num_lines))
        while next_n_lines:
            of = _get_bucket_store(f"{output_dir}/{INFILE_PREFIX}{n}")
            with of.open("w") as output_file:
                output_file.writelines(next_n_lines)
            n += 1
            next_n_lines = list(islice(input_file, split_num_lines))
            if n % 10 == 0:
                logging.info(f"Splitting in progress - created {n} partions")

    logging.info(f"Successfully split input into {n} parts")

    # Write number of split files
    metadata_file = _get_bucket_store(f"{output_dir}/{METADATA_FILENAME}")
    with metadata_file.open("w") as fh:
        fh.write(str(n))


def get_task_func(
    output_dir: str,
    bash_script: str,
    env: Optional[Dict] = None,
) -> Dict[str, BaseOperator]:
    num_files = _get_num_files(f"{output_dir}/{METADATA_FILENAME}")
    tasks = {}
    # We replace "/>" with ">" to avoid DynamicPipeOperator prematurely
    # expanding macro's that need to be expanded in the chronos task that
    # is running this BashOperator
    bash_script = bash_script.replace("\>", ">")  # noqa
    for n in range(num_files):
        tasks[f"part_{n}"] = BashOperator(
            dep_list=[],
            bash_script=f"""
bucket get {output_dir}/{INFILE_PREFIX}{n} | {bash_script}  | bucket put --ttl 86400 {output_dir}/{OUTFILE_PREFIX}{n}
            """,
            env=env,
        )
    return tasks


def join_output_func(output_dir: str):
    num_files = _get_num_files(f"{output_dir}/{METADATA_FILENAME}")

    logging.info(f"Joining script output from {num_files} files")

    f = _get_bucket_store(f"{output_dir}/{JOINED_OUT_FILENAME}")
    with f.open("w") as file_out:
        for n in range(num_files):
            input_file_part = _get_bucket_store(f"{output_dir}/{OUTFILE_PREFIX}{n}")
            with input_file_part.open("r") as part_in:
                file_out.write(part_in.read())

    logging.info("Successfully joined output data")


def distributed_script_from_bucket(
    identifier: str,
    dep_list: List[BaseOperator],
    input_bucket_path: str,
    bash_script: str,
    max_concurrent_tasks: int = 100,
    split_num_lines: Optional[int] = None,
    env: Optional[Dict] = None,
) -> Tuple[BaseOperator, BaseOperator, BaseOperator, BucketStore]:
    """
    Use distributed_script_from_manifold to process an input file in parallel with
    the provided bash script. This is often a ScriptController that accepts parameters
    from stdin.

    ### Arguments

    identifier: string, job-unique string, e.g. an ent name

    dep_list: List[DataflowOperator], usually a Presto2File

    input_manifold_path: string, path to the input file for processing

    bash_script: string, the script to run on the split input.
    `cat split_in | {script} > split_out`

    max_concurrent_tasks: int, number of bash_script instances to be run. max is 100.

    split_num_lines: Optional[int], max number of rows per file. If unspecified the
    value will be calculated based on the input length.

    env: Optional[Dict], env variables to be used in the BashOperator

    ### Returns

    split_input: BaseOperator, operator to split Manifold input into N partitions

    parallelize_bash_script: BaseOperator, DynamicPipeOperator to execute bash
    script with each input partition

    join_output: BaseOperator, optional operator to join outputs into a single handle

    output_filestore: BucketStore, joined output filestore
    """
    dir_path = f"{BASEPATH}/{identifier}"

    split_input = PythonOperator(
        dep_list=dep_list,
        func=split_input_func,
        func_args=[input_manifold_path, dir_path, split_num_lines],
    )

    # Note that DynamicPipeOperator implicitly uses ExecuteWithMacros, which
    # means macros will get expanded here, rather than in the BashOperator that
    # gets generated via _get_tasks. This can be avoided by escaping ">" with
    # "/>" in the macro.
    parallelize_bash_script = DynamicPipeOperator(
        dep_list=[split_input],
        func=get_task_func,
        func_args=[dir_path, bash_script, env],
        max_concurrent_children=max_concurrent_tasks,
    )

    join_output = PythonOperator(
        dep_list=[parallelize_bash_script],
        func=join_output_func,
        func_args=[dir_path],
    )

    output_filestore = BucketStore(
        path=f"{dir_path}/{JOINED_OUT_FILENAME}",
        retention_days=1,
        contains_pii=False,
    )

    return (
        split_input,
        parallelize_bash_script,
        join_output,
        output_filestore,
    )

