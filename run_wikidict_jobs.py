from urllib.request import urlopen
import re

from pathlib import Path
import subprocess

from multiprocessing.pool import Pool
import concurrent.futures


BASE_URL="https://dumps.wikimedia.org/enwiki/latest/"

FILENAME_MATCH="(?P<filename>enwiki-latest-pages-articles-multistream[0-9]+.xml-.*.bz2)"
DATE_MATCH="(?P<date>[0-9A-Za-z-]+)"
TIME_MATCH="(?P<time>[0-9]+:[0-9]+)"
BYTES_MATCH="(?P<bytes>[0-9]+)"

LINE_TO_MATCH=f'.*"{FILENAME_MATCH}">.*</a> +{DATE_MATCH} {TIME_MATCH} +{BYTES_MATCH}.*'.encode()


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

if __name__ == "__main__":
    # 1. Gather all dumps
    urls_to_download = []
    with urlopen(BASE_URL) as latest_wikidumps:
        for line in latest_wikidumps.readlines():
            partial_pages_dump_match = re.match(LINE_TO_MATCH, line)
            if partial_pages_dump_match:
                urls_to_download.append(partial_pages_dump_match)

    with open("./wikipedia_xml_dumps_url.list", "w") as f:
        for url_to_download in urls_to_download:
            time = url_to_download.group('time').decode()
            date = url_to_download.group('date').decode()
            filename = url_to_download.group('filename').decode()
            filesize_bytes = url_to_download.group('bytes').decode()

            f.write(f"{BASE_URL}{filename} {filename} {date}_{time} {filesize_bytes}\n")

    # 2. Read and download the dumps in parallel
    wikipedia_xml_dumps_jobs = {}
    with open("./wikipedia_xml_dumps_url.list", "r") as f:
        for line in f.readlines():
            line_match = re.match("(.*) (.*) (.*) (.*)", line)
            if line_match:
                url, filename, date_time, fbytes = line_match.groups()
                wikipedia_xml_dumps_jobs.update(
                    {
                        url: tuple([filename, date_time, int(fbytes)])
                    }
                )

    tmp = Path('tmp')
    tmp.mkdir(exist_ok=True)

    wikipedia_xml_dumps_jobs_sorted = \
            sorted(wikipedia_xml_dumps_jobs.items(), key=lambda x: x[1][2], reverse=True)

    # 2 (cont.) try to do 2 jobs at a time (downloads, parsing, etc.)
    #   concurrently despite possible bottlenecks
    #   at the time of writing there are 64 downloads to make in total
    #   so a group size of 2 is an experimental amount

    GROUP_SIZE = 8
    # task executed in a worker process
    def _job(idx):
        url, [filename, date_time, fbytes] = wikipedia_xml_dumps_jobs_sorted[idx]
        new_filename = re.sub(r'\.xml|\.bz2', '', filename)
        shell_cmd = ' '.join([
                "python",
                "./make_wikidict_online.py",
                f"<(curl {url} )",
                f"./tmp/{new_filename}__{date_time}",
        ])
        print(shell_cmd, f"# Download Size: {sizeof_fmt(fbytes)}")
        subprocess.run(
            shell_cmd,
            stdout = subprocess.PIPE, 
            stderr = subprocess.PIPE,
            text = True,
            shell = True,
            executable="/bin/bash",
        )

    with concurrent.futures.ProcessPoolExecutor(2) as executor:
        executor.map(_job, range(len(wikipedia_xml_dumps_jobs_sorted)), chunksize=GROUP_SIZE)

    from collections import Counter
    import glob
    combined_counts = Counter()
    for file in glob.glob("./tmp/*"):
        print(file)
        with open(file, 'r') as f:
            for line in f.readlines():
                word, count = line.strip().split()
                combined_counts.update({word:int(count)})

    with open("wiki_dict", 'w') as f:
        for k,v in combined_counts.most_common():
            f.write("{} {}\n".format(k,v) )
