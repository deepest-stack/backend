#!/usr/local/greenplum-db-6.10.0/ext/python/bin/python
# coding=utf-8

import tensorflow as tf

layers_map = {
    # Advanced activations.
    "leakyrelu": tf.keras.layers.LeakyReLU,
    "prelu": tf.keras.layers.PReLU,
    "elu": tf.keras.layers.ELU,
    "relu": tf.keras.layers.ReLU,
    "thresholdedrelu": tf.keras.layers.ThresholdedReLU,
    "softmax": tf.keras.layers.Softmax,

    # Convolution layers.
    "conv1d": tf.keras.layers.Conv1D,
    "conv2d": tf.keras.layers.Conv2D,
    "conv3d": tf.keras.layers.Conv3D,
    "conv2dtranspose": tf.keras.layers.Conv2DTranspose,
    "conv3dtranspose": tf.keras.layers.Conv3DTranspose,
    "separableconv1d": tf.keras.layers.SeparableConv1D,
    "separableconv2d": tf.keras.layers.SeparableConv2D,

    # Image processing layers.
    "upsampling1d": tf.keras.layers.UpSampling1D,
    "upsampling2d": tf.keras.layers.UpSampling2D,
    "upsampling3d": tf.keras.layers.UpSampling3D,
    "zeropadding1d": tf.keras.layers.ZeroPadding1D,
    "zeropadding2d": tf.keras.layers.ZeroPadding2D,
    "zeropadding3d": tf.keras.layers.ZeroPadding3D,
    "cropping1d": tf.keras.layers.Cropping1D,
    "cropping2d": tf.keras.layers.Cropping2D,
    "cropping3d": tf.keras.layers.Cropping3D,

    # Core layers.
    "masking": tf.keras.layers.Masking,
    "dropout": tf.keras.layers.Dropout,
    "spatialdropout1d": tf.keras.layers.SpatialDropout1D,
    "spatialdropout2d": tf.keras.layers.SpatialDropout2D,
    "spatialdropout3d": tf.keras.layers.SpatialDropout3D,
    "activation": tf.keras.layers.Activation,
    "reshape": tf.keras.layers.Reshape,
    "permute": tf.keras.layers.Permute,
    "flatten": tf.keras.layers.Flatten,
    "repeatvector": tf.keras.layers.RepeatVector,
    "lambda": tf.keras.layers.Lambda,
    "dense": tf.keras.layers.Dense,
    "activityregularization": tf.keras.layers.ActivityRegularization,

    # Dense Attention layers.
    "additiveattention": tf.keras.layers.AdditiveAttention,
    "attention": tf.keras.layers.Attention,

    # Embedding layers.
    "embedding": tf.keras.layers.Embedding,

    # Locally-connected layers.
    "locallyconnected1d": tf.keras.layers.LocallyConnected1D,
    "locallyconnected2d": tf.keras.layers.LocallyConnected2D,

    # Merge layers.
    "add": tf.keras.layers.Add,
    "subtract": tf.keras.layers.Subtract,
    "multiply": tf.keras.layers.Multiply,
    "average": tf.keras.layers.Average,
    "maximum": tf.keras.layers.Maximum,
    "minimum": tf.keras.layers.Minimum,
    "concatenate": tf.keras.layers.Concatenate,
    "dot": tf.keras.layers.Dot,

    # Noise layers.
    "alphadropout": tf.keras.layers.AlphaDropout,
    "gaussiannoise": tf.keras.layers.GaussianNoise,
    "gaussiandropout": tf.keras.layers.GaussianDropout,

    # Normalization layers.
    "layernormalization": tf.keras.layers.LayerNormalization,
    "batchnormalization": tf.keras.layers.BatchNormalization,

    # Pooling layers.
    "maxpooling1d": tf.keras.layers.MaxPooling1D,
    "maxpooling2d": tf.keras.layers.MaxPooling2D,
    "maxpooling3d": tf.keras.layers.MaxPooling3D,
    "averagepooling1d": tf.keras.layers.AveragePooling1D,
    "averagepooling2d": tf.keras.layers.AveragePooling2D,
    "averagepooling3d": tf.keras.layers.AveragePooling3D,
    "globalaveragepooling1d": tf.keras.layers.GlobalAveragePooling1D,
    "globalaveragepooling2d": tf.keras.layers.GlobalAveragePooling2D,
    "globalaveragepooling3d": tf.keras.layers.GlobalAveragePooling3D,
    "globalmaxpooling1d": tf.keras.layers.GlobalMaxPooling1D,
    "globalmaxpooling2d": tf.keras.layers.GlobalMaxPooling2D,
    "globalmaxpooling3d": tf.keras.layers.GlobalMaxPooling3D,

    # Recurrent layers.
    "rnn": tf.keras.layers.RNN,
    "abstractrnncell": tf.keras.layers.AbstractRNNCell,
    "stackedrnncells": tf.keras.layers.StackedRNNCells,
    "simplernncell": tf.keras.layers.SimpleRNNCell,
    "simplernn": tf.keras.layers.SimpleRNN,

    "gru": tf.keras.layers.GRU,
    "grucell": tf.keras.layers.GRUCell,
    "lstm": tf.keras.layers.LSTM,
    "lstmcell": tf.keras.layers.LSTMCell,

    # Convolutional-recurrent layers.
    "convlstm2d": tf.keras.layers.ConvLSTM2D,

    # CuDNN recurrent layers.
    "cudnnlstm": tf.keras.layers.CuDNNLSTM,
    "cudnngru": tf.keras.layers.CuDNNGRU,

    # Wrapper functions
    "wrapper": tf.keras.layers.Wrapper,
    "bidirectional": tf.keras.layers.Bidirectional,
    "timedistributed": tf.keras.layers.TimeDistributed
}

optimizers_map = {
    "sgd": tf.keras.optimizers.SGD,
    "rmsprop": tf.keras.optimizers.RMSprop,
    "adagrad": tf.keras.optimizers.Adagrad,
    "adadelta": tf.keras.optimizers.Adadelta,
    "adam": tf.keras.optimizers.Adam,
    "adamax": tf.keras.optimizers.Adamax,
    "nadam": tf.keras.optimizers.Nadam,
    "ftrl": tf.keras.optimizers.Ftrl
}

losses_map = {
    "binarycrossentropy": tf.keras.losses.BinaryCrossentropy,
    "categoricalcrossentropy": tf.keras.losses.CategoricalCrossentropy,
    "categoricalhinge": tf.keras.losses.CategoricalHinge,
    "cosinesimilarity": tf.keras.losses.CosineSimilarity,
    "hinge": tf.keras.losses.Hinge,
    "huber": tf.keras.losses.Huber,
    "kldivergence": tf.keras.losses.KLDivergence,
    "logcosh": tf.keras.losses.LogCosh,
    "loss": tf.keras.losses.Loss,
    "meanabsoluteerror": tf.keras.losses.MeanAbsoluteError,
    "meanabsolutepercentageerror": tf.keras.losses.MeanAbsolutePercentageError,
    "meansquarederror": tf.keras.losses.MeanSquaredError,
    "meansquaredlogarithmicerror": tf.keras.losses.MeanSquaredLogarithmicError,
    "poisson": tf.keras.losses.Poisson,
    "sparsecategoricalcrossentropy": tf.keras.losses.SparseCategoricalCrossentropy,
    "squaredhinge": tf.keras.losses.SquaredHinge
}

metrics_map = {
    "auc": tf.keras.metrics.AUC,
    "accuracy": tf.keras.metrics.Accuracy,
    "binaryaccuracy": tf.keras.metrics.BinaryAccuracy,
    "binarycrossentropy": tf.keras.metrics.BinaryCrossentropy,
    "categoricalaccuracy": tf.keras.metrics.CategoricalAccuracy,
    "categoricalcrossentropy": tf.keras.metrics.CategoricalCrossentropy,
    "categoricalhinge": tf.keras.metrics.CategoricalHinge,
    "cosinesimilarity": tf.keras.metrics.CosineSimilarity,
    "falsenegatives": tf.keras.metrics.FalseNegatives,
    "falsepositives": tf.keras.metrics.FalsePositives,
    "hinge": tf.keras.metrics.Hinge,
    "kldivergence": tf.keras.metrics.KLDivergence,
    "logcosherror": tf.keras.metrics.LogCoshError,
    "mean": tf.keras.metrics.Mean,
    "meanabsoluteerror": tf.keras.metrics.MeanAbsoluteError,
    "meanabsolutepercentageerror": tf.keras.metrics.MeanAbsolutePercentageError,
    "meaniou": tf.keras.metrics.MeanIoU,
    "meanrelativeerror": tf.keras.metrics.MeanRelativeError,
    "meansquarederror": tf.keras.metrics.MeanSquaredError,
    "meansquaredlogarithmicerror": tf.keras.metrics.MeanSquaredLogarithmicError,
    "meantensor": tf.keras.metrics.MeanTensor,
    "metric": tf.keras.metrics.Metric,
    "poisson": tf.keras.metrics.Poisson,
    "precision": tf.keras.metrics.Precision,
    "recall": tf.keras.metrics.Recall,
    "rootmeansquarederror": tf.keras.metrics.RootMeanSquaredError,
    "sensitivityatspecificity": tf.keras.metrics.SensitivityAtSpecificity,
    "sparsecategoricalaccuracy": tf.keras.metrics.SparseCategoricalAccuracy,
    "sparsecategoricalcrossentropy": tf.keras.metrics.SparseCategoricalCrossentropy,
    "sparsetopkcategoricalaccuracy": tf.keras.metrics.SparseTopKCategoricalAccuracy,
    "specificityatsensitivity": tf.keras.metrics.SpecificityAtSensitivity,
    "squaredhinge": tf.keras.metrics.SquaredHinge,
    "sum": tf.keras.metrics.Sum,
    "topkcategoricalaccuracy": tf.keras.metrics.TopKCategoricalAccuracy,
    "truenegatives": tf.keras.metrics.TrueNegatives,
    "truepositives": tf.keras.metrics.TruePositives
}
