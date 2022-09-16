from functools import reduce
import xml.etree.ElementTree as ET


def chunkify(iterable, len_of_chunk):
    for i in range(0, len(iterable), len_of_chunk):
        yield iterable[i : i + len_of_chunk]


def words_views(data):
    """
    Gets the word count and views per row
    """
    post_type = data.attrib.get("PostTypeId")
    if post_type == "1":
        views = int(data.attrib.get("ViewCount"))
        body = data.attrib.get("Body")
        word_count = len(body.split(" "))
        if views != 0:
            return [word_count, views]


def mapper(data):
    """
    Gets the word count sum and views sum per chunk
    """
    count_list = [0, 0]
    for x in data:
        row_count = words_views(x)
        if row_count:

            count_list[0] += row_count[0]
            count_list[1] += row_count[1]

    return count_list


def reducer(list1, list2):
    """
    Adds two elements of two lists
    """
    list_sum = [0, 0]
    list_sum[0] = list1[0] + list2[0]
    list_sum[1] = list1[1] + list2[1]

    return list_sum


if __name__ == "__main__":
    tree = ET.parse("big_data_agustina/meta_stack_overflow/posts.xml")
    root = tree.getroot()
    data_chunks = chunkify(root, 50)

    # Mapper
    mapped = list(map(mapper, data_chunks))

    # Reducer
    reduced = reduce(reducer, mapped)

    # word count vs views relation
    relation = reduced[0] / reduced[1]
    print(relation)
