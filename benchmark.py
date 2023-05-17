import re
import timeit
import subprocess
import os
import pandas as pd
from typing import List
from statistics import median

# Define the codecs to test
codecs = ['LZ4', 'ZSTD', 'BSC']
num_runs = 1  # Number of times to run each benchmark


def parse_user_space_time(input_str):
    match = re.search(r'user\s(\d+)m(\d+\.\d+)s\s', input_str)
    if not match:
        raise Exception('invalid time output')
    minutes = float(match.group(1))
    seconds = float(match.group(2))
    return 60 * minutes + seconds


def benchmark_files(files: List[str], output_format: str = 'markdown'):
    results = []

    for file in files:
        for codec in codecs:
            compress_times = []
            decompress_times = []
            for _ in range(num_runs):
                _, filename = os.path.split(file)

                # Compress the file
                compress_command = f'(time clickhouse-compressor --codec "{codec}" < {file} > {filename}-{codec}-compressed) 2> time.log'
                print(f'compress_command: {compress_command}')
                subprocess.run(compress_command, shell=True)
                with open('time.log', 'r') as f:
                    user_time = parse_user_space_time(''.join(f.readlines()))
                    compress_times.append(user_time)

                # Decompress the file and check the result
                decompress_command = f'(time clickhouse-compressor --decompress --codec "{codec}" < {filename}-{codec}-compressed > {filename}-{codec}-decompressed) 2> time.log'
                subprocess.run(decompress_command, shell=True)
                with open('time.log', 'r') as f:
                    user_time = parse_user_space_time(''.join(f.readlines()))
                    decompress_times.append(user_time)

            # Calculate the compression rate
            initial_size = os.path.getsize(file)
            compressed_size = os.path.getsize(f'{filename}-{codec}-compressed')
            compression_rate = compressed_size / initial_size

            # Check if the decompressed file matches the original
            match = subprocess.run(f'diff {file} {filename}-{codec}-decompressed',
                                   shell=True).returncode == 0

            # Store the result
            results.append({
                'File': file,
                'Codec': codec,
                'Compress Time (s)': median(compress_times),
                'Decompress Time (s)': median(decompress_times),
                'Compression Rate': compression_rate,
                'Match': match
            })

    # Create a pandas DataFrame from the results
    df = pd.DataFrame(results)

    # Output the results in the specified format
    if output_format == 'csv':
        df.to_csv('benchmark_results.csv', index=False)
    elif output_format == 'markdown':
        print(df.to_markdown())
    else:
        print(df)


if __name__ == '__main__':
    # '../store/7f2/7f28c961-5abb-4436-a2ff-329a2d8d0dac/201403_1_31_2/Title.bin'
    benchmark_files(['tiny-data.txt'])