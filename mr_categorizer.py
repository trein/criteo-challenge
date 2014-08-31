from collections import defaultdict
from disco.core import Job, result_iterator
from json import dumps


CATEGORY_MAPPING_OUT = '/Users/trein/datasets/criteo/data/mr_categorical_mapping.json'
CATEGORY_STATUS_OUT = '/Users/trein/datasets/criteo/data/mr_categorical_status.json'
TRAIN_IN = '/Users/trein/datasets/criteo/train_noheader.csv'
MAX_CATEGORICAL_FEATURES = 500


def mapper(line, params):
    category_columns = xrange(15, 40)
    feature_vector = line.split(',')
    for cat_id in category_columns:
        cat_value = feature_vector[cat_id]
        yield cat_id, cat_value


def reducer(iter, params):
    from disco.util import kvgroup
    from collections import Counter

    for cat_id, values in kvgroup(iter):
        yield cat_id, Counter(values)


def main():
    job = Job().run(input=[TRAIN_IN], map=mapper, reduce=reducer, sort=True)
    categories_options = defaultdict(dict)
    category_values = defaultdict(int)
    for cat_id, counter in result_iterator(job.wait(show=True)):
        if len(counter) > MAX_CATEGORICAL_FEATURES:
            continue

        for cat_value in counter:
            if cat_value not in categories_options[cat_id]:
                categories_options[cat_id][cat_value] = category_values[cat_id]
                category_values[cat_id] += 1

    # save possible categorical data
    with open(CATEGORY_MAPPING_OUT, 'w') as f:
        f.write(dumps(categories_options))

    with open(CATEGORY_STATUS_OUT, 'w') as f:
        f.write(dumps(category_values))


if __name__ == '__main__':
    main()
