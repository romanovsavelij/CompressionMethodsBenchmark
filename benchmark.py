import re
import subprocess
import os
import pandas as pd
from typing import List
from statistics import median
import base64
import unicodedata
import re

# Define the codecs to test
# 'LZ4HC(4)', 'LZ4HC(6)', 'LZ4HC(9)', 'LZ4HC(12)'
# 'ZSTD(1)', 'ZSTD(5)', 'ZSTD(10)', 'ZSTD(16)', 'ZSTD(22)'
# 'DEFLATE_QPL'
codecs = ['BSC']
num_runs = 3  # Number of times to run each benchmark


def parse_user_space_time(input_str):
    match = re.search(r'(\d+)\.(\d+)user', input_str)
    if not match:
        raise Exception('invalid time output')
    seconds = float(match.group(1))
    second_frac = float(match.group(2))
    return seconds + (second_frac / 100)


def file_size_mb(filename):
    st = os.stat(filename)
    return st.st_size / (1024 * 1024)


def to_filename(s):
    return slugify(s)

def slugify(value, allow_unicode=False):
    """
    Taken from https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.
    """
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
    else:
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value.lower())
    return re.sub(r'[-\s]+', '-', value).strip('-_')


def benchmark_files(files: List[str], output_format: str = 'markdown'):
    results = []

    for file in files:
        print(f"file: {file}")
        print(f"size: {file_size_mb(file)} mbs")
        for codec in codecs:
            compress_speed = []
            decompress_speed = []
            for _ in range(num_runs):
                _, filename = os.path.split(file)

                # Compress the file
                compress_input_file = file
                compress_output_file = to_filename(f"{filename}-{codec}-compressed")
                compress_command = f'(numactl --physcpubind=3 time clickhouse-compressor --codec "{codec}" < {compress_input_file} > {compress_output_file}) 2> time.log'
                print(f'compress_command: {compress_command}')
                subprocess.run(compress_command, shell=True)
                with open('time.log', 'r') as f:
                    user_time = parse_user_space_time(''.join(f.readlines()))
                    print(f"compress time: {user_time}")
                    compress_speed.append(file_size_mb(compress_input_file) / user_time)

                # Decompress the file and check the result
                decompress_input_file = compress_output_file
                decompress_output_file = to_filename(f"{filename}-{codec}-decompressed")
                decompress_command = f'(numactl --physcpubind=3 time clickhouse-compressor --decompress --codec "{codec}" < {decompress_input_file} > {decompress_output_file}) 2> time.log'
                print(f'decompress_command: {decompress_command}')
                subprocess.run(decompress_command, shell=True)
                with open('time.log', 'r') as f:
                    user_time = parse_user_space_time(''.join(f.readlines()))
                    print(f"decompress time: {user_time}")
                    decompress_speed.append(file_size_mb(compress_input_file) / user_time)

            # Calculate the compression rate
            initial_size = os.path.getsize(file)
            compressed_size = os.path.getsize(compress_output_file)
            compression_rate = compressed_size / initial_size

            # Check if the decompressed file matches the original
            match = subprocess.run(f'diff {file} {decompress_output_file}',
                                   shell=True).returncode == 0
            if not match:
                raise Exception(f'compressed-decompressed data does not equal original for codec={codec}, file={filename}')

            # Store the result
            results.append({
                'File': filename,
                'Codec': codec,
                'Compress Speed (mb/s)': median(compress_speed),
                'Decompress Speed (mb/s)': median(decompress_speed),
                'Ratio': 1.0 / compression_rate,
            })

    # Create a pandas DataFrame from the results
    df = pd.DataFrame(results)
    df.reset_index(drop=True, inplace=True)
    df.to_csv('benchmark_results.csv', index=False)

    # Output the results in the specified format
    if output_format == 'markdown':
        print(df.to_markdown())
    else:
        print(df)


if __name__ == '__main__':
    # 'tiny-data.txt'
    # 'all-titles.bin', 'all-watch-ids-dup.bin', 'all-event-times-dup.bin', 'all-referer-regions-dup.bin'
    benchmark_files(['all-titles.bin', 'all-watch-ids-dup.bin', 'all-event-times-dup.bin', 'all-referer-regions-dup.bin'])