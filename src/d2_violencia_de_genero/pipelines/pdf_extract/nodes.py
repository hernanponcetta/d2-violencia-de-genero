from fuzzysearch import find_near_matches


def extract_information(pages_list):

    string_1 = "corte suprema de justicia de la nación"
    string_2 = "oficina de violencia doméstica"

    pages_dict = {}

    for idx, text in enumerate(pages_list):

        matches_1 = find_near_matches(string_1, text.lower(), max_l_dist=4)
        matches_2 = find_near_matches(string_2, text.lower(), max_l_dist=4)

        if matches_1 and matches_2:
            pages_dict[idx] = text

    return pages_dict


def filter_pages(page_dict, dist=10):

    page_number = list(page_dict.keys())
    page_list = []

    for i in range(len(page_number) - 1):
        if (page_number[i + 1] - page_number[i]) < dist:
            page_list.append(page_number[i])
        elif (i > 0) and (page_number[i] - page_number[i - 1] < dist):
            page_list.append(page_number[i])

    return sorted(page_list)


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
