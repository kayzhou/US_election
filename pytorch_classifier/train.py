# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    train.py                                           :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/04/24 14:41:26 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/05/08 08:57:22 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import torch
from fastNLP import (AccuracyMetric, CrossEntropyLoss, DataSet, Trainer,
                     Vocabulary)
from fastNLP.embeddings import StaticEmbedding
from fastNLP.io import DataBundle
from fastNLP.modules import LSTM

from torch import nn
from torch.optim import Adam


def load_data(in_name):
    _data = []
    for line in open(in_name):
        target, tokens = line.strip().split("\t")
        _data.append({"target": int(target), "tokens": tokens.split()})
    return _data


data = load_data("data/tokens.train/tokens.04.txt")
targets = [d["target"] for d in data]
tokens = [d["tokens"] for d in data]
seq_len = [len(d["tokens"]) for d in data]

dataset = DataSet({'chars': tokens, 'target': targets, 'seq_len': seq_len})
# print(dataset)

vocab = Vocabulary()
#  从该dataset中的chars列建立词表
vocab.from_dataset(dataset, field_name='chars')
#  使用vocabulary将chars列转换为index
vocab.index_dataset(dataset, field_name='chars')
embed = StaticEmbedding(vocab, model_dir_or_name=None, embedding_dim=300)

target_vocab = Vocabulary(padding=None, unknown=None)
target_vocab.from_dataset(dataset, field_name='target')
target_vocab.index_dataset(dataset, field_name='target')

data_bundle = DataBundle(datasets={"train": dataset}, 
                         vocabs={"target": target_vocab})
print(data_bundle)


# 定义模型
class BiLSTMMaxPoolCls(nn.Module):
    def __init__(self, embed, num_classes, hidden_size=400, 
                 num_layers=1, dropout=0.3):
        super().__init__()
        self.embed = embed

        self.lstm = LSTM(self.embed.embedding_dim,
                         hidden_size=hidden_size//2, 
                         num_layers=num_layers,
                         batch_first=True,
                         bidirectional=True)
        self.dropout_layer = nn.Dropout(dropout)
        self.fc = nn.Linear(hidden_size, num_classes)

    def forward(self, chars, seq_len):
        # chars:[batch_size, max_len]
        # seq_len: [batch_size, ]
        chars = self.embed(chars)
        outputs, _ = self.lstm(chars, seq_len)
        outputs = self.dropout_layer(outputs)
        outputs, _ = torch.max(outputs, dim=1)
        outputs = self.fc(outputs)

        return {'pred': outputs}  # [batch_size,]


# print(len(data_bundle.get_vocab('target')))

# 初始化模型
model = BiLSTMMaxPoolCls(embed, len(data_bundle.get_vocab('target')))
loss = CrossEntropyLoss()
optimizer = Adam(model.parameters(), lr=0.01)
metric = AccuracyMetric()
device = 0 if torch.cuda.is_available() else 'cpu'  # 如果有gpu的话在gpu上运行，训练速度会更快

trainer = Trainer(train_data=data_bundle.get_dataset('train'), 
                  model=model,
                  loss=loss,
                  optimizer=optimizer, 
                  batch_size=32, 
                  dev_data=data_bundle.get_dataset('train'),
                  metrics=metric, 
                  device=device)   
trainer.train()  # 开始训练，训练完成之后默认会加载在dev上表现最好的模型

# 在测试集上测试一下模型的性能
# from fastNLP import Tester
# print("Performance on test is:")
# tester = Tester(data=data_bundle.get_dataset('test'), model=model, metrics=metric, batch_size=64, device=device)
# tester.test()
