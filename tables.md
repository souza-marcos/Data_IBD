# TP2 IDB

## Tabelas:

### Empresa
Atributo | Tipo | Descrição
---------|------|------------
CNPJ | String | CNPJ da empresa
Razão Social | String | Razão social da empresa
Instalação | String | Instalação da empresa 
LocalidadeFK | int | Código da localidade da empresa

OBS: Assumindo que um CNPJ mapeia para apenas uma instalação.

### Localidade
Atributo | Tipo | Descrição
---------|------|------------
ID | int | Código da localidade
Região | String | Região da localidade
Estado | String | Estado da localidade
Município | String | Município da localidade

### Processamento de Petróleo
Atributo | Tipo | Descrição
---------|------|------------
ID | int | Código do processamento
Mês | int | Mês do processamento
Ano | int | Ano do processamento
EmpresaFK | String | CNPJ da empresa
Capacidade | int | Capacidade autorizada de processamento da empresa
Processado | int | Volume

--- 
## Específicos para Refinaria

### Produto 
Atributo | Tipo | Descrição
---------|------|------------
ID | int | Código do produto
Nome | String | Nome do produto
Unidade | String | Unidade do produto (m³, kg, etc)

### Refinamento 
Atributo | Tipo | Descrição
---------|------|------------
ID | int | Código do refinamento
Mês | int | Mês do refinamento
Ano | int | Ano do refinamento
EmpresaFK | String | CNPJ da empresa
ProdutoFK | int | Código do produto
Producao | int | Qtd produzida

--- 
## Específicos para Central Petroquímica



