from fuzzysearch import find_near_matches


def extract_information(partitions):

    parts = []

    for part in partitions.items():

        string_1 = "corte suprema de justicia de la nación"
        string_2 = "oficina de violencia doméstica"

        pages_dict = {}

        for idx, text in enumerate(pages_list):

            matches_1 = find_near_matches(string_1, text.lower(), max_l_dist=4)
            matches_2 = find_near_matches(string_2, text.lower(), max_l_dist=4)

            if matches_1 and matches_2:
                pages_dict[idx] = text

        parts.append(pages_dict)

    return parts


def filter_pages(partintions, dist=10):

    parts = []

    for part in partintions:

        page_number = list(page_dict.keys())
        page_list = []

        for i in range(len(page_number) - 1):
            if (page_number[i + 1] - page_number[i]) < dist:
                page_list.append(page_number[i])
            elif (i > 0) and (page_number[i] - page_number[i - 1] < dist):
                page_list.append(page_number[i])

        parts.append(sorted(page_list))

    return parts


def filt_dict(pages_dict, pages_list):

    filt_pages = {}

    for page_number in pages_dict:
        if pages_list[0] <= page_number <= pages_list[-1] + 1:
            filt_pages[page_number] = pages_dict[page_number]

    return filt_pages


def filt_list(pages_dict, pages_list):

    filt_pages = []

    for page_number in pages_dict:
        if pages_list[0] <= page_number <= pages_list[-1] + 1:
            filt_pages.append(pages_dict[page_number])

    return filt_pages
