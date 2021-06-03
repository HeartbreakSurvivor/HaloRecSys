from NeuralCF import NCF
import torch
import torch.nn as nn
import pandas as pd
import torch.utils.data as Data

from Trainer import Trainer

ncf_config = {
    'num_epoch': 2,
    'batch_size': 256,
    'optimizer': 'adam',
    'lr': 0.001,
    'num_users': 1074,
    'num_items': 982,
    'latent_dim_gmf': 8,
    'latent_dim_mlp': 8,
    'layers': [16,64,32,8],  # layers[0] is the concat of latent user vector & latent item vector
    'l2_regularization': 0.01,
    'device_id': 0,
    'use_cuda': True,
    'train_file': './newRatings.csv',
    'model_name': './TrainedModels/NCF.pth'
}

if __name__ == "__main__":
    ####################################################################################
    # load training data
    ####################################################################################
    # we don't need the first line
    df = pd.read_csv(ncf_config["train_file"], sep=',', engine='python')
    print(df.columns)

    training_data, training_label = df.drop(columns='score').values, df['score'].values
    train_dataset = Data.TensorDataset(torch.tensor(training_data), torch.tensor(training_label))

    # print(training_data)
    # print(training_label)
    ####################################################################################
    # load NCF model
    ####################################################################################
    NCF = NCF(ncf_config)
    print("NCF: ", NCF)

    ####################################################################################
    # model training
    ####################################################################################
    # trainer = Trainer(model=NCF, config=ncf_config)
    # # 训练
    # trainer.train(train_dataset)
    # # 保存模型
    # trainer.save()

    ####################################################################################
    # 模型测试阶段
    ####################################################################################
    NCF.eval()
    if ncf_config['use_cuda']:
        NCF.loadModel(map_location=lambda storage, loc: storage.cuda(ncf_config['device_id']))
        NCF = NCF.cuda()
    else:
        NCF.loadModel(map_location=torch.device('cpu'))

    y_pred_probs = NCF(torch.tensor([1,1,1,1,1]).cuda(), torch.tensor([7,12,26,32,40]).cuda())
    #y_pred = torch.where(y_pred_probs>0.5, torch.ones_like(y_pred_probs), torch.zeros_like(y_pred_probs))
    print("predictes shape: ", y_pred_probs.shape)
    print("predictes: ", y_pred_probs)
    print("Test Data CTR Predict...\n ", y_pred_probs.view(-1))
    res = [y.item() for y in y_pred_probs.detach().cpu().numpy()]
    print(res)
    print(type(res[0]))

