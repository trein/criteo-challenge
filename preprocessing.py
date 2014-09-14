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

selected_categorical_features = ['C1', 'C2', 'C5', 'C6', 'C8', 'C9', 'C14', 'C17', 'C20', 'C22', 'C23', 'C25']
selected_categorical_features_first = ['C6', 'C9', 'C14', 'C17', 'C20', 'C22', 'C23', 'C25']
selected_categorical_features_sec = ['C5', 'C6', 'C8', 'C9', 'C14', 'C17', 'C20', 'C22', 'C23', 'C25']
selected_categorical_features_red = ['C6', 'C9', 'C14', 'C17', 'C20', 'C22', 'C23', 'C25']


class DataSetBuilder(object):
    def __init__(self):
        with gzip.open(constants.CATEGORY_MAPPING_OUT, 'r') as f:
            self.category_mapping = json.loads(f.read())

        with gzip.open(constants.CATEGORY_STATUS_OUT, 'r') as f:
            self.category_status = json.loads(f.read())

        with gzip.open(constants.INT_STATUS_OUT, 'r') as f:
            self.int_status = json.loads(f.read())

    @constants.timed
    def build(self):
        print 'Starting data set assembly...'

        print 'Building training set'
        train_c_id_to_index = constants.convert_train_c_id_to_index
        train_i_id_to_index = constants.convert_train_i_id_to_index
        limit = constants.LIMIT_CATEGORICAL
        train_raw_file = constants.TRAIN_RAW
        train_expanded_file = constants.TRAIN_EXPANDED
        self._expand_dataset(train_raw_file, train_expanded_file, train_c_id_to_index, train_i_id_to_index, limit)

        print 'Building test set'
        test_c_id_to_index = constants.convert_test_c_id_to_index
        test_i_id_to_index = constants.convert_test_i_id_to_index
        limit = constants.LIMIT_CATEGORICAL - 1
        test_raw_file = constants.TEST_RAW
        test_expanded_file = constants.TEST_EXPANDED
        self._expand_dataset(test_raw_file, test_expanded_file, test_c_id_to_index, test_i_id_to_index, limit)

    def _expand_dataset(self, raw_file, expanded_file, cat_conversion_func, int_conversion_func, limit):
        with gzip.open(raw_file, 'r') as source, gzip.open(expanded_file, 'w') as destination:
            reader = csv.reader(source)
            writer = csv.writer(destination)

            # adjusting header
            header = reader.next()[:limit]
            for cat_id in selected_categorical_features:
                options = self.category_mapping[cat_id]
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
                    cat_index = cat_conversion_func(cat_id)
                    options = self.category_mapping[cat_id]
                    options_len = len(options)
                    options_features = [0] * options_len

                    feature_value = feature_vector[cat_index]
                    option_index = options.get(feature_value, 0)
                    options_features[option_index] = 1
                    expanded_feature_vector += options_features

                for int_id in constants.INTEGER_IDS:
                    int_index = int_conversion_func(int_id)
                    int_status = self.int_status[int_id]
                    raw_current_value = expanded_feature_vector[int_index]
                    current_value = float(raw_current_value) if raw_current_value != '' else 0.0
                    # current_min = int_status['min']
                    current_max = int_status['max']
                    current_sum = int_status['sum']
                    current_count = int_status['count']

                    if raw_current_value != '':
                        expanded_feature_vector[int_index] = current_value / current_max
                    else:
                        expanded_feature_vector[int_index] = current_sum / current_count / current_max
                writer.writerow(expanded_feature_vector)


class DataSetAnalysis(object):
    @constants.timed
    def process(self):
        print 'Starting data set analysis...'

        category_mapping = {cat_id: {} for cat_id in constants.CATEGORIES_IDS}
        category_status = {cat_id: 0 for cat_id in constants.CATEGORIES_IDS}
        int_status = {int_id: {'max': 0.0, 'min': 1e4, 'count': 0.0, 'sum': 0.0} for int_id in constants.INTEGER_IDS}

        self._worker(category_mapping, category_status, int_status)
        self._save(category_mapping, category_status, int_status)

    def _save(self, category_mapping, category_status, int_status):
        # save possible categorical values
        with gzip.open(constants.CATEGORY_MAPPING_OUT, 'w') as f:
            f.write(json.dumps(category_mapping))

        with gzip.open(constants.CATEGORY_STATUS_OUT, 'w') as f:
            f.write(json.dumps(category_status))

        with gzip.open(constants.INT_STATUS_OUT, 'w') as f:
            f.write(json.dumps(int_status))

    def _worker(self, category_mapping, category_status, int_status):
        n_sample = 0
        with gzip.open(constants.TRAIN_RAW, 'r') as f:
            reader = csv.reader(f)
            next(reader, None)
            for feature_vector in reader:

                n_sample += 1
                if n_sample % 500000 == 0:
                    print 'Processing sample [%s]' % n_sample
                    print 'Current values of categories %s' % category_status

                for int_id in constants.CATEGORIES_IDS:
                    cat_index = constants.convert_train_c_id_to_index(int_id)
                    if feature_vector[cat_index] not in category_mapping[int_id]:
                        category_mapping[int_id][feature_vector[cat_index]] = category_status[int_id]
                        category_status[int_id] += 1

                for int_id in constants.INTEGER_IDS:
                    int_index = constants.convert_train_i_id_to_index(int_id)
                    raw_current_value = feature_vector[int_index]
                    current_value = float(raw_current_value) if raw_current_value != '' else 0.0
                    current_min = int_status[int_id]['min']
                    current_max = int_status[int_id]['max']
                    if raw_current_value != '':
                        int_status[int_id]['count'] += 1
                        if current_value > current_max:
                            int_status[int_id]['max'] = current_value
                        if current_value < current_min:
                            int_status[int_id]['min'] = current_value
                    int_status[int_id]['sum'] += current_value


def main():
    # DataSetAnalysis().process()
    DataSetBuilder().build()


if __name__ == '__main__':
    main()
