import argparse
import os

from spex.common.job_manager import JobManager
from spex.pipelines.example import create_fancy_pipeline


def run(job):
    tx_data = job.read("member_transactions")
    member_info = job.read("member_info")

    pipeline = create_fancy_pipeline(member_info)
    output = pipeline(tx_data)

    print(f"Resulting rows: {output.count()}.")

    print("DONE")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            """
            Digital offers sorting and assignment DQ checks
            """
        )
    )

    parser.add_argument(
        "--env",
        type=str,
        default="dev",
        help=(
            """
            environment to use (dev, qa, prod) - defaults to dev
            """
        ),
    )

    config_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../conf/config{env}.yaml",
    )

    args = parser.parse_args()

    job = JobManager(config_path=config_path.format(env=f"_{args.env}"))
    run(job)
