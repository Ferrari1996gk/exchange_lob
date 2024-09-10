import numpy as np
import pandas as pd
from datetime import datetime
import os
import pickle
import torch
from sklearn import metrics
import torch.nn as nn
from torch.utils.data import DataLoader
import torch.optim as optim
import matplotlib.pyplot as plt
torch.manual_seed(0)
print('Required library imported!')

def get_day_feature_label(data, seq_len=100):
    df_data = pd.DataFrame(data)
    isna = df_data['trades'].isna()
    df_data.loc[isna, 'trades'] = pd.Series([[0 for i in range(seq_len)]] * isna.sum()).values
    df_data.dropna(inplace=True)
    df_train = df_data.drop('is_spike', axis=1)
    df_test = df_data['is_spike']

    flag = True
    for col in feature_list:
        if col in ["trades", "events"]:
            tmp_arr = pd.DataFrame(df_train[col].values.tolist()).fillna(0, axis=1).values
        else:
            tmp_arr = pd.DataFrame(df_train[col].values.tolist()).fillna(method='ffill', axis=1).values
        tmp_arr = np.expand_dims(tmp_arr, axis=-1)
        if flag:
            res_arr = tmp_arr
            flag = False
        else:
            res_arr = np.concatenate((res_arr,tmp_arr), axis=-1)
    return res_arr, df_test.values

def accuracy(output, target):
    assert len(output) == len(target), "output and target must have same length!"
    length = len(target)
    target = target.float()
    output = output.view(target.shape)
    predict = torch.round(output)
    correct = (predict == target).float()
    acc = correct.sum() / length

    y_true = np.array(target)
    y_pred = predict.detach().numpy()

    matrix = metrics.confusion_matrix(y_true, y_pred)
    f1_score = metrics.f1_score(y_true, y_pred)
    return acc, f1_score, matrix

class RNNClassifier(nn.Module):
    def __init__(self, input_size=19, out_size=1, hidden_size=32, num_layers=2, bidirection=False, model_name='LSTM'):
        super().__init__()
        if model_name == 'LSTM':
            self.rnn = nn.LSTM(input_size=input_size, hidden_size=hidden_size, num_layers=num_layers,
                               bidirectional=bidirection)
        elif model_name == 'GRU':
            self.rnn = nn.GRU(input_size=input_size, hidden_size=hidden_size, num_layers=num_layers,
                              bidirectional=bidirection)
        else:
            raise Exception('Model name should be either\'LSTM\' or \'GRU\'!')

        if bidirection:
            self.fc = nn.Linear(in_features=hidden_size*2, out_features=out_size)
        else:
            self.fc = nn.Linear(in_features=hidden_size, out_features=out_size)
        self.sigmoid = nn.Sigmoid()

    def forward(self, input):
        permuted = input.permute([1, 0, 2])
        states, hidden = self.rnn(permuted)
        encoding = states[-1]
        out = self.sigmoid(self.fc(encoding))
        return out


def get_rnn_model(lr=0.01, hidden_size=32, num_layers=2, bidirection=False, input_size=19, out_size=1, model_name='LSTM'):
    model = RNNClassifier(input_size=input_size, hidden_size=hidden_size, num_layers=num_layers,
                          bidirection=bidirection, out_size=out_size, model_name=model_name)
    optimizer = optim.Adam(model.parameters(), lr=lr)
    loss_fn = nn.BCELoss()
    return model, optimizer, loss_fn

def divided_model_pred(model, sent_tensor, single_size=100):
    res_list = []
    num = len(sent_tensor) // single_size
    if (len(sent_tensor) % single_size) != 0:
        num += 1
    for i in range(num):
        print(i)
        feature = sent_tensor[i * single_size: (i + 1) * single_size]
        predictions = model(feature)
        res_list.append(predictions)
    return torch.vstack(res_list)

directory = "../../notebooks/predictive_model/data/"
datestr_list = [x[:8] for x in os.listdir(directory)]
feature_list = ['mid', 'spread', 'events', 'trades', 'imbalance_level_0', 'imbalance_level_1', 'imbalance_level_2',
                'imbalance_level_3', 'imbalance_level_4', 'buy_gap_0', 'buy_gap_1', 'buy_gap_2', 'buy_gap_3',
                'buy_gap_4', 'sell_gap_0', 'sell_gap_1', 'sell_gap_2', 'sell_gap_3', 'sell_gap_4']

flag = True
for datestr in datestr_list:
    print(datestr)
    with open('%s%s.pkl'%(directory, datestr), 'rb') as fp:
        data = pickle.load(fp)
        fp.close()
    if flag:
        feature_arr, label_arr = get_day_feature_label(data)
        flag = False
    else:
        tmp_feature, tmp_label = get_day_feature_label(data)
        feature_arr = np.concatenate([feature_arr, tmp_feature])
        label_arr = np.concatenate([label_arr, tmp_label])
    print(feature_arr.shape, ' ', label_arr.shape)

total_num = len(label_arr)
test_percent = 0.25
split = int(total_num * (1 - test_percent))
train_feature, train_label = feature_arr[:split], label_arr[:split]
test_feature, test_label = feature_arr[split:], label_arr[split:]
print(train_feature.shape, train_label.shape)
print(test_feature.shape, test_label.shape)
# shuffle
p = np.random.permutation(len(train_label))
train_feature, train_label = train_feature[p], train_label[p]
p = np.random.permutation(len(test_label))
test_feature, test_label = test_feature[p], test_label[p]
# Balance
# idx_list = []
# length = (train_label == 1).sum()
# idx_list.extend(np.where(train_label == 1)[0])
# idx_list.extend(np.where(train_label == 0)[0][:length])
# train_feature, train_label = train_feature[idx_list], train_label[idx_list]
idx_list = []
pos = (train_label == 1).sum()
neg = (train_label == 0).sum()
quo, rem = neg // pos, neg % pos
for i in range(quo):
    idx_list.extend(np.where(train_label == 1)[0])
idx_list.extend(np.where(train_label == 1)[0][:rem])
idx_list.extend(np.where(train_label == 0)[0])
train_feature, train_label = train_feature[idx_list], train_label[idx_list]

# idx_list = []
# length = (test_label == 1).sum()
# idx_list.extend(np.where(test_label == 1)[0])
# idx_list.extend(np.where(test_label == 0)[0][:length])
# test_feature, test_label = test_feature[idx_list], test_label[idx_list]
print(train_feature.shape, train_label.shape)
print(test_feature.shape, test_label.shape)
# shuffle again
p = np.random.permutation(len(train_label))
train_feature, train_label = train_feature[p], train_label[p]
p = np.random.permutation(len(test_label))
test_feature, test_label = test_feature[p], test_label[p]

hidden_size = 64
bidirection = False
balance = False
num_layers = 5
lr = 0.0003
epoch_num = 40
batch_size = 128
model_name = 'LSTM'
model, optimizer, loss_fn = get_rnn_model(lr=lr, hidden_size=hidden_size, num_layers=num_layers,
                                           bidirection=bidirection, model_name=model_name)
initial_train_data, initial_label_data = torch.from_numpy(train_feature).float(), torch.from_numpy(train_label).float()
test_sent_tensor, test_label_tensor = torch.from_numpy(test_feature).float(), torch.from_numpy(test_label).float()

train_acc_list = []
test_acc_list = []
train_f1_list = []
test_f1_list = []
batch_num = len(initial_label_data) // batch_size
if (len(initial_label_data) % batch_size) != 0:
    batch_num += 1
for epoch in range(1, epoch_num + 1):
    p = np.random.permutation(len(initial_label_data))
    train_sent_tensor, train_label_tensor = initial_train_data[p], initial_label_data[p]
    # print('train_label_tensor: ', train_label_tensor[:50])
    model.train()
    for i in range(batch_num):
        if i % 50 == 0:
            print("training process: %d / %d / %d" % (i, batch_num, len(initial_label_data)))
        feature = train_sent_tensor[i * batch_size: (i + 1) * batch_size]
        target = train_label_tensor[i * batch_size: (i + 1) * batch_size]
        # Zero the gradients
        optimizer.zero_grad()
        predictions = model(feature)
        # print(predictions.shape)
        predictions = predictions.view(target.shape)
        # print(predictions.shape)
        # print(target.shape)
        # predictions[predictions != predictions] = 0.5
        loss = loss_fn(predictions, target)
        loss.backward()
        optimizer.step()
    model.eval()
    with torch.no_grad():
        # pred_train = divided_model_pred(model, initial_train_data, 500)
        # pred_test = divided_model_pred(model, test_sent_tensor, 500)
        pred_train = model(initial_train_data)
        pred_test = model(test_sent_tensor)
        train_acc, train_f1score, train_matrix = accuracy(pred_train, initial_label_data)
        test_acc, test_f1score, test_matrix = accuracy(pred_test, test_label_tensor)
    # print('Epoch: %3d | Test accuracy: %.2f%%' % (epoch, test_acc * 100))
    # print('Epoch: %3d | Test f1_score: %.2f' % (epoch, test_f1score))
    print('Epoch: %3d | Train accuracy: %.2f%% | Test accuracy: %.2f%%' % (epoch, train_acc * 100, test_acc * 100))
    print('Epoch: %3d | Train f1_score: %.2f | Test f1_score: %.2f' % (epoch, train_f1score, test_f1score))
    print('Train confusion matrix: ')
    print(train_matrix)
    print('Test confusion matrix: ')
    print(test_matrix)
    train_acc_list.append(train_acc)
    test_acc_list.append(test_acc)
    train_f1_list.append(train_f1score)
    test_f1_list.append(test_f1score)


fig, ax = plt.subplots(figsize=(15, 5))
ax.plot(train_acc_list, label="training accuracy")
ax.plot(test_acc_list, label="testing accuracy")
ax.tick_params(axis='both', which='both', labelsize=18)
ax.set_xlabel("training epochs", fontsize=25)
ax.set_ylabel("Accuracy", fontsize=25)
ax.set_title("LSTM model Train-test accuracy", fontsize=30)
# ax.set_ylim((0,.02))
ax.legend(loc='best', fontsize='xx-large')
plt.show()

fig, ax = plt.subplots(figsize=(15, 5))
ax.plot(train_f1_list, label="training f1 score")
ax.plot(test_f1_list, label="testing f1 score")
ax.tick_params(axis='both', which='both', labelsize=18)
ax.set_xlabel("training epochs", fontsize=25)
ax.set_ylabel("f1 score", fontsize=25)
ax.set_title("LSTM model Train-test classification f1 score", fontsize=30)
# ax.set_ylim((0,.02))
ax.legend(loc='best', fontsize='xx-large')
plt.show()