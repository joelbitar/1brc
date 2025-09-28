from collections import defaultdict
from dataclasses import dataclass
import asyncio
from typing import Optional


@dataclass
class Place:
    min_temp: float = -99.0
    max_temp: float = 99.0
    sum_temp: float = 0.0
    temp_count: int = 0
    avg_temp: Optional[float] = None


@dataclass
class Result:
    places: list[Place]


@dataclass
class Chunk:
    lines: list[str]
    size: int

    def __str__(self):
        return f"Chunk(lines={self.size})"


def get_file_parts(chunk_line_count: int = 10000):
    with open("../data/measurements.txt", 'rt') as f:
        chunk = []

        for index, line in enumerate(f):
            chunk.append(line)

            if (index + 1) % chunk_line_count == 0:
                yield Chunk(chunk, chunk_line_count)
                chunk = []

        # yield the last chunk
        if size := len(chunk):
            yield Chunk(
                chunk,
                size
            )


def get_places(chunk: Chunk):
    places = defaultdict(Place)
    for line in chunk.lines:
        name, temp = line.split(";")
        places[name].min_temp = min(places[name].min_temp, float(temp))
        places[name].min_temp = max(places[name].min_temp, float(temp))
        places[name].sum_temp += float(temp)
        places[name].temp_count += 1

    return places


def execute():
    places = defaultdict(Place)

    for chunk in get_file_parts():
        result = get_places(chunk)
        for name, r in result.items():
            places[name].min_temp = min(places[name].min_temp, r.min_temp)
            places[name].max_temp = max(places[name].max_temp, r.max_temp)
            places[name].sum_temp += r.sum_temp
            places[name].temp_count += r.temp_count

    for place, temps in places.items():
        places[place].avg_temp = temps.sum_temp / temps.temp_count

    print(places)

if __name__ == "__main__":
    print("starting...")
    execute()
    print("finished.")
