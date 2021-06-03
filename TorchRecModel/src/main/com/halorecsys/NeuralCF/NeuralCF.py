import torch
import torch.nn as nn

class NCF(nn.Module):
    def __init__(self, config):
        nn.Module.__init__(self)

        self._config = config
        self._num_users = config['num_users']
        self._num_items = config['num_items']
        self._latent_dim_gmf = config['latent_dim_gmf']
        self._latent_dim_mlp = config['latent_dim_mlp']

        # 建立MLP模型的user Embedding层和item Embedding层，输入的向量长度分别为用户的数量，item的数量，输出维度为latent dim的embeddinng向量
        self._embedding_user_mlp = torch.nn.Embedding(num_embeddings=self._num_users, embedding_dim=self._latent_dim_mlp)
        self._embedding_item_mlp = torch.nn.Embedding(num_embeddings=self._num_items, embedding_dim=self._latent_dim_mlp)

        # 建立GMP模型的user Embedding层和item Embedding层，输入的向量长度分别为用户的数量，item的数量，输出维度为latent dim的embeddinng向量
        self._embedding_user_gmf = torch.nn.Embedding(num_embeddings=self._num_users, embedding_dim=self._latent_dim_gmf)
        self._embedding_item_gmf = torch.nn.Embedding(num_embeddings=self._num_items, embedding_dim=self._latent_dim_gmf)

        # 全连接层
        self._fc_layers = torch.nn.ModuleList()
        for in_size, out_size in zip(config['layers'][:-1], config['layers'][1:]):
            self._fc_layers.append(torch.nn.Linear(in_size, out_size))

        # 创建一个线性模型，输入为GMF模型和MLP模型的潜在特征向量长度之和，输出向量长度为1
        self._affine_output = torch.nn.Linear(in_features=config['layers'][-1] + self._latent_dim_gmf, out_features=1)

        # 激活函数
        self._logistic = nn.Sigmoid()

    def forward(self, user_idx, item_idx):
        # construct user and item embedding vector
        user_embedding_mlp = self._embedding_user_mlp(user_idx)
        item_embedding_mlp = self._embedding_item_mlp(item_idx)
        user_embedding_gmf = self._embedding_user_gmf(user_idx)
        item_embedding_gmf = self._embedding_item_gmf(item_idx)

        # concat the two latent vector
        mlp_vector = torch.cat([user_embedding_mlp, item_embedding_mlp], dim=-1)

        # multiply the two latent vector
        gmf_vector = torch.mul(user_embedding_gmf, item_embedding_gmf)

        # through a mlp layer
        for idx, _ in enumerate(range(len(self._fc_layers))):
            mlp_vector = self._fc_layers[idx](mlp_vector)
            mlp_vector = torch.nn.ReLU()(mlp_vector)

        vector = torch.cat([mlp_vector, gmf_vector], dim=-1)
        logits = self._affine_output(vector)
        rating = self._logistic(logits)
        return rating

    def dataPreProcess(self, x):
        user_idx, item_idx = x.split([1,1], dim=1) # 根据第一维将原始数据分为两个Tensor
        return user_idx, item_idx

    def loadModel(self, map_location):
        state_dict = torch.load(self._config['model_name'], map_location=map_location)
        self.load_state_dict(state_dict, strict=False)

    def saveModel(self):
        torch.save(self.state_dict(), self._config['model_name'])