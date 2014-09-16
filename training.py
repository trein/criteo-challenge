import csv
import constants
import gzip

from sklearn import linear_model
from sklearn.externals import joblib


class Trainer(object):
    def __init__(self, load=False):
        if load:
            self.clf = joblib.load(constants.MODEL_FILENAME)
        else:
            self.clf = linear_model.SGDRegressor(learning_rate='constant', verbose=3, alpha=1e-5, eta0=1e-4)

    @constants.timed
    def train(self):
        with gzip.open(constants.TRAIN_EXPANDED, 'r') as source:
            reader = csv.reader(source)
            next(reader, None)

            n_sample = 0
            labels = []
            features = []
            for feature_vector in reader:
                s_features = feature_vector[2:6] + feature_vector[7:]
                s_label = int(feature_vector[1])
                features.append(s_features)
                labels.append(s_label)

                # print 'features', s_features
                # print 'labels', s_label
                # print 'norm features', normalized_features

                n_sample += 1
                if n_sample % 500000 == 0:
                    self.clf.partial_fit(features, labels)
                    features = []
                    labels = []
                    print 'Processing sample [%s]' % n_sample

        print 'Finished training'
        print 'Estimated parameters [%s]' % self.clf.get_params()

        # saving model into file
        joblib.dump(self.clf, constants.MODEL_FILENAME, compress=9)


    @constants.timed
    def validate(self):
        with gzip.open(constants.VALIDATION_EXPANDED, 'r') as source:
            reader = csv.reader(source)
            next(reader, None)

            n_sample = 0
            for feature_vector in reader:
                s_features = [float(i) for i in feature_vector[2:6] + feature_vector[7:]]
                s_label = int(feature_vector[1])

                n_sample += 1
                s_result = self.clf.predict([s_features])
                s_predicted = s_result[0] if s_result[0] > 0 else 0.0

                print 'Processing sample [%s]' % n_sample
                print 'Predicted [%s] for label [%s]' % (s_predicted, s_label)
                print 'Score [%s]' % llfun([s_label], [s_predicted])

    @constants.timed
    def evaluate(self):
        with gzip.open(constants.TEST_EXPANDED, 'r') as source:
            with gzip.open(constants.EVALUATION, 'w') as destination:
                reader = csv.reader(source)
                writer = csv.writer(destination)

                next(reader, None)
                writer.writerow(['Id', 'Predicted'])

                n_sample = 0
                for feature_vector in reader:
                    n_sample += 1
                    s_features = [float(i) for i in feature_vector[1:5] + feature_vector[6:]]
                    s_id = int(feature_vector[0])
                    s_result = self.clf.predict([s_features])
                    s_predicted = s_result[0] if s_result[0] > 0 else 0.0
                    writer.writerow([s_id, s_predicted])

                    print 'Processing sample [%s] - ctr [%s]' % (n_sample, s_predicted)


import scipy as sp


def llfun(act, pred):
    epsilon = 1e-15
    pred = sp.maximum(epsilon, pred)
    pred = sp.minimum(1 - epsilon, pred)
    ll = sum(act * sp.log(pred) + sp.subtract(1, act) * sp.log(sp.subtract(1, pred)))
    ll = ll * -1.0 / len(act)
    return ll


def main():
    import preprocessing
    # preprocessing.DataSetAnalysis().process()
    preprocessing.DataSetBuilder().build()

    Trainer().train()
    # Trainer(load=True).validate()
    Trainer(load=True).evaluate()


if __name__ == '__main__':
    main()
