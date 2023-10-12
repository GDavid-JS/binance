import os
import time
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense, Dropout, BatchNormalization
from tensorflow.keras.optimizers import Adam, SGD, RMSprop, Adamax
from tensorflow.keras.callbacks import EarlyStopping
from sklearn.model_selection import train_test_split

from data import transfer_data, DataNN

def prepare_data(data, inp_len, X_list, y_list):
    n = data.shape[0] - inp_len
    if n > 0:
        for i in range(n):
            X = np.array(data[i:i + inp_len], dtype=np.uint8)
            Y = np.array(data[i+inp_len], dtype=np.uint8)

            X_list.append(X)
            y_list.append(Y)
    
def main():
    USER = os.environ.get('POSTGRES_USER')
    PASSWORD = os.environ.get('POSTGRES_PASSWORD')
    HOST = os.environ.get('HOST')
    PORT = os.environ.get('POSTGRES_PORT')
    SOURCE_NAME = os.environ.get('NAME1')
    TARGET_NAME = os.environ.get('NAME2')

    base_params = {
        'user': USER,
        'password': PASSWORD,
        'host': HOST,
        'port': PORT,
    }

    source_database = {**base_params, 'database': SOURCE_NAME}
    database = {**base_params, 'database': TARGET_NAME}
    input_path = os.path.abspath(f'neural_networks/model3')
    output_path = os.path.abspath(f'neural_networks/model3')

    # transfer_data(source_database, database)

    inp_len = 100
    timeframe = '30m'
    coin = 'btcusdt'
    num_epochs = 500
    batch_size = 256
    early_stopping_patience=10

    # , return_sequences=True
    # model = Sequential([
    #     LSTM(256, input_shape=(inp_len, 1), return_sequences=True),
    #     LSTM(128),
    #     Dense(1, activation='sigmoid')
    # ])

    model = Sequential([
        LSTM(256, input_shape=(inp_len, 1), activation='relu'),
        Dense(1, activation='sigmoid')
    ])

    # model = Sequential([
    #     LSTM(64, input_shape=(inp_len, 1)),
    #     # LSTM(100),
    #     Dense(1, activation='sigmoid')
    # ])


    # model = load_model(input_path)


    model.summary()
    optimizer = Adam(learning_rate=0.002)
    model.compile(optimizer=optimizer, loss='binary_crossentropy', metrics=['accuracy'])


    dataNN = DataNN(**database)
    start_time = time.time()
    data = dataNN.get_data(coin, timeframe)
    # data_gen = dataNN.get_timeframe_data(timeframe)

    X_list = []
    y_list = []

    # for data in data_gen:
    #     prepare_data(data, inp_len, X_list, y_list)
    prepare_data(data, inp_len, X_list, y_list)

        
    
    print('Data len: ', len(X_list))
    
    X_array = np.array(X_list)
    y_array = np.array(y_list)

    end_time = time.time()
    print("Data loading time:", end_time - start_time)

    X_train, X_temp, y_train, y_temp = train_test_split(X_array, y_array, test_size=0.3, random_state=42)
    X_val, X_test, y_val, y_test = train_test_split(X_temp, y_temp, test_size=0.5, random_state=42)

    # , restore_best_weights=True
    early_stopping = EarlyStopping(monitor='val_accuracy', patience=early_stopping_patience)

    history = model.fit(X_train, y_train, validation_data=(X_val, y_val), epochs=num_epochs, batch_size=batch_size, verbose=1)
    # , callbacks=[early_stopping]

    loss, accuracy = model.evaluate(X_test, y_test)


    model.save(output_path)

if __name__ == '__main__':
    # print("Num GPUs Available: ", len(tf.config.list_physical_devices('GPU')))
    with tf.device('/GPU:0'):
        main()
    pass
