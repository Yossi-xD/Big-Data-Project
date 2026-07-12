import argparse
import csv
import random
from pathlib import Path


def create_sample(
    input_path: Path,
    output_path: Path,
    sample_size: int,
    seed: int,
) -> None:
    random_generator = random.Random(seed)
    sample = []

    with input_path.open("r", encoding="utf-8-sig", newline="") as source_file:
        reader = csv.DictReader(source_file)

        if reader.fieldnames is None:
            raise ValueError("The input CSV does not contain a header.")

        for row_number, row in enumerate(reader, start=1):
            if row_number <= sample_size:
                sample.append(row)
            else:
                replacement_index = random_generator.randint(1, row_number)
                if replacement_index <= sample_size:
                    sample[replacement_index - 1] = row

    if len(sample) < sample_size:
        raise ValueError(
            f"Requested {sample_size} rows, but the source contains only "
            f"{len(sample)} rows."
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(sample)

    print(f"Created {output_path} with {len(sample)} rows.")
    print(f"Random seed: {seed}")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a reproducible random sample from TrafficTab23."
    )
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--rows", type=int, default=20_000)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_arguments()
    create_sample(
        input_path=arguments.input,
        output_path=arguments.output,
        sample_size=arguments.rows,
        seed=arguments.seed,
    ) 