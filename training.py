import csv
import constants
import gzip

from sklearn import linear_model
from sklearn import preprocessing
from sklearn.externals import joblib


MODEL_FILENAME = 'criteo/data/model.pkl'


class Trainer(object):
    def __init__(self, load=False):
        if load:
            self.clf = joblib.load(MODEL_FILENAME)
        else:
            self.clf = linear_model.SGDRegressor(alpha=1e-7)

    @constants.timed
    def train(self):
        first = True
        scaler = preprocessing.StandardScaler()
        with gzip.open(constants.TRAIN_EXPANDED, 'r') as source:
            reader = csv.reader(source)
            next(reader, None)

            n_sample = 0
            labels = []
            features = []
            for feature_vector in reader:
                # ad_id = feature_vector[0]
                s_features = []
                for f in feature_vector[2:]:
                    s_features.append(float(f) if f != '' else 0.0)
                s_label = int(feature_vector[1])
                features.append(s_features)
                labels.append(s_label)

                # print 'features', s_features
                # print 'labels', s_label
                # print 'norm features', normalized_features

                n_sample += 1
                if n_sample % 100000 == 0:
                    if first:
                        normalized_features = scaler.fit_transform(features)
                        first = False
                    else:
                        normalized_features = scaler.transform(features)

                    self.clf.partial_fit(normalized_features, labels)
                    features = []
                    labels = []
                    print 'Processing sample [%s]' % n_sample

        print 'Finished training'
        print 'Estimator parameters [%s]' % self.clf.get_params()

        # saving model into file
        joblib.dump(self.clf, MODEL_FILENAME, compress=9)


def main():
    Trainer().train()


if __name__ == '__main__':
    main()
