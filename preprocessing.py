import json
import csv
import constants
import gzip

HEADER = ('Id,Label,'
          'I1,I2,I3,I4,I5,I6,I7,I8,I9,I10,I11,I12,I13,'
          'C1,C2,C3,C4,C5,C6,C7,C8,C9,C10,C11,C12,C13,C14,C15,C16,C17,C18,C19,C20,C21,C22,C23,C24,C25,C26')

c_features_status = {
    "C19": 1303, "C18": 2548, "C13": 2796, "C12": 41312, "C11": 3799, "C10": 10997, "C17": 10,
    "C16": 34617, "C15": 5238, "C14": 26, "C9": 3, "C8": 257, "C3": 43870, "C2": 497, "C1": 541,
    "C7": 7623, "C6": 12, "C5": 145, "C4": 25184, "C22": 11, "C23": 14, "C20": 4, "C21": 38618,
    "C26": 9527, "C24": 12335, "C25": 51
}

selected_categorical_features = [
    'C8',
    'C5',
    'C14',
    'C17',
    'C20',
    'C22',
    'C23',
    'C25',
]


class DataBuilder(object):
    def __init__(self):
        with gzip.open(constants.CATEGORY_MAPPING_OUT, 'r') as f:
            self.category_options = json.loads(f.read())

        with open(constants.CATEGORY_STATUS_OUT, 'r') as f:
            self.category_values = json.loads(f.read())

    @constants.timed
    def build(self):
        train_c_id_to_index = constants.convert_train_c_id_to_index
        limit = constants.LIMIT_CATEGORICAL
        self._expand_dataset(constants.TRAIN_RAW, constants.TRAIN_EXPANDED, train_c_id_to_index, limit)

        test_c_id_to_index = constants.convert_test_c_id_to_index
        limit = constants.LIMIT_CATEGORICAL - 1
        self._expand_dataset(constants.TEST_RAW, constants.TEST_EXPANDED, test_c_id_to_index, limit)

    def _expand_dataset(self, raw_file, expanded_file, conversion_func, limit):
        with gzip.open(raw_file, 'r') as source:
            with gzip.open(expanded_file, 'w') as destination:
                reader = csv.reader(source)
                writer = csv.writer(destination)

                # adjusting header
                header = reader.next()[:limit]
                for cat_id in selected_categorical_features:
                    options = self.category_options[cat_id]
                    options_len = len(options)
                    options_features = [None] * options_len

                    for option_value, option_index in options.iteritems():
                        options_features[option_index] = '%s=%s' % (cat_id, option_value)
                    header += options_features

                writer.writerow(header)

                # adjusting features
                n_sample = 0
                for feature_vector in reader:
                    expanded_feature_vector = feature_vector[:limit]

                    n_sample += 1
                    if n_sample % 500000 == 0:
                        print 'Processing sample [%s]' % n_sample

                    for cat_id in selected_categorical_features:
                        cat_index = conversion_func(cat_id)
                        options = self.category_options[cat_id]
                        options_len = len(options)
                        options_features = [0] * options_len

                        feature_value = feature_vector[cat_index]
                        option_index = options.get(feature_value, '')
                        options_features[option_index] = 1
                        expanded_feature_vector += options_features

                    writer.writerow(expanded_feature_vector)


class Categorizer(object):
    @constants.timed
    def process(self):
        category_options = {cat_id: {} for cat_id in constants.CATEGORIES_IDS}
        category_values = {cat_id: 0 for cat_id in constants.CATEGORIES_IDS}
        self._worker(category_options, category_values)
        self._save(category_options, category_values)

    def _save(self, category_options, category_values):
        # save possible categorical values
        with gzip.open(constants.CATEGORY_MAPPING_OUT, 'w') as f:
            f.write(json.dumps(category_options))

        with gzip.open(constants.CATEGORY_STATUS_OUT, 'w') as f:
            f.write(json.dumps(category_values))

    def _worker(self, categories_options, category_values):
        n_sample = 0
        with gzip.open(constants.TRAIN_RAW, 'r') as f:
            reader = csv.reader(f)
            next(reader, None)
            for feature_vector in reader:

                n_sample += 1
                if n_sample % 500000 == 0:
                    print 'Processing sample [%s]' % n_sample
                    print 'Current values of categories %s' % category_values

                for cat_id in constants.CATEGORIES_IDS:
                    cat_index = constants.convert_train_c_id_to_index(cat_id)
                    if feature_vector[cat_index] not in categories_options[cat_id]:
                        categories_options[cat_id][feature_vector[cat_index]] = category_values[cat_id]
                        category_values[cat_id] += 1


def main():
    Categorizer().process()
    DataBuilder().build()


if __name__ == '__main__':
    main()
