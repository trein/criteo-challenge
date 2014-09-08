import csv
import constants
import gzip

from sklearn import linear_model
from sklearn.externals import joblib


MODEL_FILENAME = 'criteo/data/model.pkl'


class Trainer(object):
    def __init__(self, load=False):
        if load:
            self.clf = joblib.load(MODEL_FILENAME)
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
        joblib.dump(self.clf, MODEL_FILENAME, compress=9)


    @constants.timed
    def validate(self):
        with gzip.open(constants.VALIDATION_EXPANDED, 'r') as source:
            reader = csv.reader(source)
            next(reader, None)

            n_sample = 0
            for feature_vector in reader:
                s_features = feature_vector[2:6] + feature_vector[7:]
                s_label = int(feature_vector[1])

                n_sample += 1
                target, shape = self.clf.predict([s_features])

                print 'Processing sample [%s]' % n_sample
                print 'Predicted [%s] for label [%s]' % (target[0], s_label)


def main():
    Trainer().train()
    # Trainer(load=True).validate()


if __name__ == '__main__':
    main()
