import sys
import os
from os import listdir
import datetime
import matplotlib.pyplot as plt
import numpy as np
from numpy import vstack
from numpy import sqrt
from sklearn.metrics import mean_squared_error
import torch
from torch.utils.data import Dataset
from torch.utils.data import DataLoader
from torch.utils.data import random_split
from torch import nn
from torch import Tensor
from torch.nn import Linear
from torch.nn import ReLU
from torch.nn import Sigmoid
from torch.nn import Module
from torch.optim import SGD
from torch.nn import MSELoss
from torch.nn import CrossEntropyLoss
from torch.nn.init import xavier_uniform_
from torchvision import transforms
from TezCountersBase import TezCountersBase, VertexProperties

NUM_INPUT = 5
NUM_OUTPUT = 2
BYTE_DIVIDE=1024*1024
TIME_DIVIDE=1000 * 60


class TezCountersData(Dataset):

    def __init__(self, workDirs):
        self.exps = []
        self.cwd = os.getcwd()
        self.vertexData = []

        for wd in workDirs:
            results = []
            exists = os.path.isfile(wd + "/data.npy")
            if exists:
                results = np.load(wd + "/data.npy")

            else:
                tez = TezCountersBase(wd)
                for vertex in tez.vertexResults:
                    result = []
                    result.append(self.codeVertexManager(vertex[VertexProperties.index('vertexManagerName')]))
                    result.append(vertex[VertexProperties.index('runTime')]//TIME_DIVIDE)
                    #result.append(vertex[VertexProperties.index('HDFS_BYTES_READ')])
                    #result.append(vertex[VertexProperties.index('HDFS_BYTES_WRITTEN')])
                    result.append(vertex[VertexProperties.index('FILE_BYTES_READ')]//BYTE_DIVIDE)
                    result.append(vertex[VertexProperties.index('FILE_BYTES_WRITTEN')]//BYTE_DIVIDE)
                    result.append(vertex[VertexProperties.index('SHUFFLE_BYTES')]//BYTE_DIVIDE)
                    result.append(vertex[VertexProperties.index('avgTaskCPUutil')])
                    result.append(vertex[VertexProperties.index('SPILLED_RECORDS')])
                    results.append(result.copy())

                del tez
                np.save(wd + "/data.npy", np.array(results))

            self.vertexData.extend(results)
            os.chdir(self.cwd)

        self.vertexData = np.array(self.vertexData)
        print(self.vertexData.shape)
        
        self.X = self.vertexData[:,1:NUM_INPUT].astype('float32')
        #self.normalize(self.X)
        self.X = np.append(self.X, self.vertexData[:,:1].astype('float32'), axis=1)

        self.Y = self.vertexData[:,NUM_INPUT:].astype('float32')
        #self.normalize(self.Y)
        self.Y = self.Y.reshape((len(self.Y), 2))


#        transform = transforms.Compose([
#          transforms.ToTensor(),
#        transforms.Normalize(mean=[.5],
#                         std=[1])
#        ])


    def __len__(self):
        return len(self.X)

    def __getitem__(self, idx):
        return [self.X[idx], self.Y[idx]]

    def get_splits(self, n_test=0.33):
        # determine sizes
        test_size = round(n_test * len(self.X))
        train_size = len(self.X) - test_size
        # calculate the split
        return random_split(self, [train_size, test_size])


    def codeVertexManager(self, manager):
        if manager == "RootInputVertexManager":
            return 0

        if manager == "ShuffleVertexManager":
            return 1

    def normalize(self, data):
        means = np.mean(data, axis = 0)
        stds = np.var(data, axis = 0)
        data = (data-means)/stds


class NNmodel(nn.Module):
    def __init__(self, n_inputs):
        super(NNmodel, self).__init__()

        self.hidden1 = Linear(n_inputs, NUM_INPUT*NUM_OUTPUT*NUM_INPUT)
        #xavier_uniform_(self.hidden1.weight)
        #self.act_rel1 = ReLU()
        self.act_rel1 = Sigmoid()
        self.hidden2 = Linear(NUM_OUTPUT*NUM_INPUT*NUM_INPUT, NUM_INPUT)
        #xavier_uniform_(self.hidden2.weight)
        self.act_rel2 = ReLU()
#        self.act_rel2 = Sigmoid()
        self.hidden3 = Linear(NUM_INPUT, NUM_OUTPUT)
        #self.hidden3 = Linear(NUM_INPUT, 1)
        #xavier_uniform_(self.hidden3.weight)
        #self.act_sig = Sigmoid()

    def forward(self, X):
        X = self.hidden1(X)
        X = self.act_rel1(X)
        X = self.hidden2(X)
        X = self.act_rel2(X)
        X = self.hidden3(X)

        return X
                
# prepare the dataset
def prepare_data(workDirs):
    # load the dataset
    dataset = TezCountersData(workDirs)
    # calculate split
    train, test = dataset.get_splits()
    # prepare data loaders
    train_dl = DataLoader(train, batch_size=len(train), shuffle=True)
    test_dl = DataLoader(test, batch_size=32, shuffle=False)
    return train_dl, test_dl

# train the model
def train_model(train_dl, model):
    # define the optimization
    criterion = MSELoss()
#    criterion = CrossEntropyLoss()
    optimizer = SGD(model.parameters(), lr=0.01, momentum=0.9)
    # enumerate epochs
    for epoch in range(20):
        print('Training epoch ', epoch)
        # enumerate mini batches
        for i, (inputs, targets) in enumerate(train_dl):
            # clear the gradients
            optimizer.zero_grad()
            #print('input: ', inputs)
            # compute the model output
            yhat = model(inputs)
#            print('model output: ', yhat)
            # calculate loss
            loss = criterion(yhat, targets)
            # credit assignment
            loss.backward()
            # update model weights
            optimizer.step()
            #print('target: ', targets)
            print('batch ', i, ', loss: ', loss)
        #print(yhat)
    print('Finished training')

# evaluate the model
def evaluate_model(test_dl, model):
    predictions, actuals = list(), list()
    for i, (inputs, targets) in enumerate(test_dl):
        # evaluate the model on the test set
        print('input: ', inputs)
        yhat = model(inputs)
        #print(yhat)
        print('model op: ', yhat)
        # retrieve numpy array
        yhat = yhat.detach().numpy()
        actual = targets.numpy()
        print('target: ', actual)
        #actual = actual.reshape((len(actual), 1))
        # round to class values
        #yhat = yhat.round()
        # store
        predictions.append(yhat)
        actuals.append(actual) 
        break
    predictions, actuals = vstack(predictions), vstack(actuals)
    # calculate accuracy
    #print(actuals)
    #print(predictions)
    mse = mean_squared_error(actuals, predictions)
    return mse

# make a class prediction for one row of data
def predict(row, model):
    # convert row to data
    row = Tensor([row])
    # make prediction
    yhat = model(row)
    # retrieve numpy array
    yhat = yhat.detach().numpy()
    return yhat

def main():

    workDirs = []

    if len(sys.argv) < 2:
        print("Please provide at least two dirs")
        return 

    for i in range(1, len(sys.argv)):
        workDirs.append(sys.argv[i])

    train_dl, test_dl = prepare_data(workDirs)
    print(len(train_dl.dataset), len(test_dl.dataset))

    model = NNmodel(NUM_INPUT)
    print(model)
    #return
    train_model(train_dl, model)
    # evaluate the model
    #mse = evaluate_model(test_dl, model)
    #model.eval()
#    mse = evaluate_model(train_dl, model)
#    print('MSE: %.3f, RMSE: %.3f' % (mse, sqrt(mse)))
#    row = [0, 7641, 104457151, 0, 8672, 130778, 0]
 #   yhat = predict(row, model)
 #   print(yhat)



if __name__ == "__main__":
    main()
