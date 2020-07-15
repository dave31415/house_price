from collections import defaultdict, Counter
from pomegranate import BayesianNetwork


def get_links_from_data(data):
    fields = None
    links = defaultdict(set)
    for row_num, row in enumerate(data):
        print(row_num)
        if fields is None:
            fields = list(row.keys())
        for field_1 in fields:
            value_1 = row[field_1]
            key_pair_1 = (field_1, value_1)
            for field_2 in fields:
                if field_1 != field_2:
                    value_2 = row[field_2]
                    key_pair_2 = (field_2, value_2)
                    links[key_pair_1].add(key_pair_2)
                    links[key_pair_2].add(key_pair_1)

    return links, fields


def count_card(value_set):
    counts = Counter()
    for k, v in value_set:
        counts[k] += 1

    return counts


def field_pair_max(data):
    links, fields = get_links_from_data(data)

    max_for_field_pair = defaultdict(float)
    for key_1, value_set in links.items():
        field_1, val_1 = key_1
        counts = count_card(value_set)
        for field_2, num in counts.items():
            field_pair = (field_1, field_2)
            value = max(max_for_field_pair[field_pair], num)
            max_for_field_pair[field_pair] = value

    return max_for_field_pair


def partial_order(data):
    max_field_pair = field_pair_max(data)
    pairs = [k for k, v in max_field_pair.items() if v == 1]
    equivalent_pairs, ordered_pairs = group_ordering(pairs)
    return equivalent_pairs, ordered_pairs


def group_ordering(partial_ordering):
    equivalent_pairs = []
    ordered_pairs = []
    for pair in partial_ordering:
        field_1, field_2 = pair
        pair_flipped = (field_2, field_1)
        if pair_flipped in partial_ordering:
            if field_1 < field_2:
                # choose only 1 ordering, alphabetical
                equivalent_pairs.append(pair)
        else:
            ordered_pairs.append(pair)

    return equivalent_pairs, ordered_pairs


def choose_order(equivalent_pairs):
    ordered_pairs = []
    for pair in equivalent_pairs:
        print(pair)
        index = int(input('Choose index for primary field, 0 or 1'))
        assert index in [0, 1]
        if index == 0:
            ordered_pairs.append(pair)
        else:
            field_1, field_2 = pair
            pair_flipped = (field_2, field_1)
            ordered_pairs.append(pair_flipped)

    return ordered_pairs


def get_test_data():
    data = [{'cust_id': 0, 'name': 'Bill Jones', 'city': 'Boston', 'city_population': 100000, 'item': 'bat'},
            {'cust_id': 1, 'name': 'Sara Smith', 'city': 'Boston', 'city_population': 100000, 'item': 'ball'},
            {'cust_id': 2, 'name': 'Roger Rose', 'city': 'New York', 'city_population': 900000, 'item': 'glove'},
            {'cust_id': 3, 'name': 'Mike Manning', 'city': 'Chicago', 'city_population': 770000, 'item': 'glove'},
            {'cust_id': 0, 'name': 'Bill Jones', 'city': 'Boston', 'city_population': 100000, 'item': 'glove'},
            {'cust_id': 0, 'name': 'Bill Jones', 'city': 'Boston', 'city_population': 100000, 'item': 'hat'},
            {'cust_id': 1, 'name': 'Sara Smith', 'city': 'Boston', 'city_population': 100000, 'item': 'hat'},
            {'cust_id': 1, 'name': 'Sara Smith', 'city': 'Boston', 'city_population': 100000, 'item': 'glove'},
            {'cust_id': 2, 'name': 'Roger Rose', 'city': 'New York', 'city_population': 900000, 'item': 'bat'},
            {'cust_id': 3, 'name': 'Mike Manning', 'city': 'Chicago', 'city_population': 770000, 'item': 'bat'},
            {'cust_id': 4, 'name': 'Bill Jones', 'city': 'Grand Rapids', 'city_population': 22200, 'item': 'bat'},
            {'cust_id': 1, 'name': 'Sara Smith', 'city': 'Boston', 'city_population': 100000, 'item': 'ball'}]

    return data


def test_partial_order():
    data = get_test_data()
    data = 100 * data

    equiv, ordered = partial_order(data)

    assert equiv == [('city', 'city_population')]
    assert ordered == [('cust_id', 'city_population'), ('cust_id', 'name'), ('cust_id', 'city')]

    return equiv, ordered


def row_to_list(row, fields):
    return [str(row[f]) for f in fields]


def data_to_matrix(data, fields):
    return [row_to_list(row, fields) for row in data]


def test_pom():
    data = get_test_data()
    data = 4 * data

    fields = list(data[0].keys())
    data_matrix = data_to_matrix(data, fields)

    network = BayesianNetwork.from_samples(data_matrix,
                                           algorithm='exact',
                                           pseudocount=0)

    example = [['1', 'Sara Smith', 'Boston', '100000', 'bat'],
               ['1', 'Sara Smith', 'Boston', '100000', 'ball'],
               ['1', 'Sara Smith', 'Boston', '100000', 'hat'],
               ['1', 'Sara Smith', 'Boston', '100000', 'glove']]

    prob = network.probability(example)
    prob /= prob.sum()
    print(prob)

    return network


def value_counts(data_frame):
    fields = list(data_frame.columns)
    return {field: len(data_frame[field].value_counts()) for field in fields}


def strip_fields(data, field_list):
    return [{k: v for k, v in row.items() if k not in field_list} for row in data]
