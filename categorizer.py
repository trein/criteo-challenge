import json
import csv


# BASE_DIR = '/Users/trein/datasets/criteo/'
BASE_DIR = 'criteo/'

CATEGORY_MAPPING_OUT = BASE_DIR + 'data/categorical_mapping.json'
CATEGORY_STATUS_OUT = BASE_DIR + 'data/categorical_status.json'
TRAIN_IN = BASE_DIR + 'train.csv'

C_COLUMNS = range(15, 40)


def worker(categories_options, category_values):
    counter = 0
    with open(TRAIN_IN, 'r') as f:
        reader = csv.reader(f)
        next(reader, None)
        for feature_vector in reader:

            counter += 1
            if counter % 500000 == 0:
                print 'Processing line [%s]' % counter
                print 'Current values of categories %s' % category_values

            for cat_id in C_COLUMNS:
                if feature_vector[cat_id] not in categories_options[cat_id]:
                    categories_options[cat_id][feature_vector[cat_id]] = category_values[cat_id]
                    category_values[cat_id] += 1


def main():
    categories_options = {cat_id: {} for cat_id in C_COLUMNS}
    category_values = {cat_id: 0 for cat_id in C_COLUMNS}

    worker(categories_options, category_values)

    # save possible categorical values
    with open(CATEGORY_MAPPING_OUT, 'w') as f:
        f.write(json.dumps(categories_options))
    with open(CATEGORY_STATUS_OUT, 'w') as f:
        f.write(json.dumps(category_values))


if __name__ == '__main__':
    main()
