from functools import reduce
import xml.etree.ElementTree as ET
from collections import Counter
import re


def chunkify(iterable, len_of_chunk):
    """Divides the dataset in smaller pieces

    Parameters
    ----------
    iterable : iterable

    len_of_chunk: int
        The length of each chunks

    """
    for i in range(0, len(iterable), len_of_chunk):
        yield iterable[i : i + len_of_chunk]


def unanswered_responses(data):
    """extracts the tags of unanswered questions

    Parameters
    ----------
    data : list

    Returns
    -------
    tag_list : a list of tags if the question is unanswered

    """
    post_type = data.attrib.get("PostTypeId")
    # Post type 1 is the only one with tags
    if post_type == "1":
        accepted_id = data.attrib.get("AcceptedAnswerId")
        tags = data.attrib.get("Tags")
        tag_list = re.findall("<(.+?)>", tags)
        # If not accepted id is found, returns tag list
        if not accepted_id:
            return tag_list


def mapper(data):
    """Creates a dictionary of tags for every chunk that is processed

    Parameters
    ----------
    data : list

    Returns
    -------
    tags_dict : a dictionary with the count of how many times a tag appears in that chunk

    """
    acc_tags = []
    for x in data:
        tag_list = unanswered_responses(x)
        if tag_list is not None:
            acc_tags.append(tag_list)
    # Flatten list of list to a single list
    data_process = []
    for list_tag in acc_tags:
        for tag in list_tag:
            data_process.append(tag)
    # Generate a dict with tags and count for every chunk
    tags_dict = dict(Counter(data_process))
    return tags_dict


def reducer(dict1, dict2):
    """Joins the two dict of tags into one"""
    for key, value in dict2.items():
        if key in dict1.keys():
            dict1[key] = dict1[key] + value
        else:
            dict1[key] = value
    return dict1


if __name__ == "__main__":
    tree = ET.parse("big_data_agustina/meta_stack_overflow/posts.xml")
    root = tree.getroot()
    data_chunks = chunkify(root, 100)

    # Map
    mapped = list(map(mapper, data_chunks))

    # Reduce. Combines all dict into one
    final_tag_dict = reduce(reducer, mapped)

    top_10_tags = dict(Counter(final_tag_dict).most_common(10))
    print(top_10_tags)
