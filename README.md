Database
===============

Database é um *bitcode* de acesso a dados em Bando de Dados Relacional (SQL DB) para [thrust](https://github.com/thrustjs/thrust).

# Instalação

Posicionado em um app [thrust](https://github.com/thrustjs/thrust), no seu terminal:

```bash
thrust install database
```

# Tutorial

Para utilizar as APIs de acesso a banco de dados é necessário importar o bitcode 'database'
```javascript
    let dbm = require('database')
```

Defina as opções do pool de conexões. Uma boa prática é colocar esta configuração no arquivo config.json que
fica no diretório raiz da aplicação
```javascript
    let dbConfig = getBitcodeConfig('database')()
```

Depois execute a função de criação do objeto que disponibilizará as APIs de banco de dados
```javascript
    let db = dbm.createDbInstance(dbConfig)
```

Agora ficou fácil. É só utilizar os métodos de acesso à dados para construir sua aplicação
```javascript
    let rs

    rs = db.execute('DROP TABLE IF EXISTS "ttest"')

    if (rs.error === false) {
        rs = db.execute('CREATE TABLE "ttest" ("key" INTEGER PRIMARY KEY AUTOINCREMENT, "num" NUMERIC, "txt" VARCHAR(64)) ')

        if (rs.error === false) {
            rs = db.execute('INSERT INTO "ttest" ("num", "txt") values (1, 'Num Um')', true)
            show('Array de chaves dos registros inseridos: ', rs.keys)

            let regs = [{ num: 10, txt: 'Num Dez' }, { num: 11, txt: 'Num Onze' }, { num: 12, txt: 'Num Doze' }]

            rs = db.insert('ttest', regs)
            show('Array de chaves dos registros inseridos: ', rs.keys)

            rs = db.execute('SELECT * FROM "ttest" WHERE "num" = :numero AND "txt" = :texto', {numero: 11, texto: 'Num Onze'})
            show('Result =>', rs)

            rs = db.select('SELECT * FROM "ttest" WHERE "num" = :numero AND "txt" = :texto', {numero: 10, texto: 'Num Dez'})
            show('Result =>', rs)

            // executando vários comandos dentro de uma mesma transação
            rs = db.executeInSingleTransaction(function (db, context) {
                let cmd = 'INSERT INTO "ttest" ("num", "txt") values (6, \'Num Seis\'), ' +
                    ' (7, 'Num Sete'), (8, 'Num Oito'), (9, 'Num Nove')'

                db.execute(cmd)
                db.execute('UPDATE "ttest" SET "num" = :num, "txt" = :txt WHERE "num"=9', context)
            }, {num: 99, txt: 'Num Noventa e Nove'})

        }
    }

```
## Parâmetros de configuração
As propriedades abaixo devem ser configuradas no arquivo *config.json*:

``` javascript
{
  ...
  'database': { /*Configuração de um database*/
    'driverClassName': /*String Class do driver de conexão*/,
    'urlConnection': /*String Url de conexão com o banco*/,
    'userName': /*String Usuário do banco*/,
    'password': /*String Senha do banco*/,

    /* As configurações abaixo são opcionais
    e possuem os defaults apresentados*/
    'initialSize': 5,
    'maxActive': 15,
    'maxIdle': 7,
    'minIdle': 3,
  }
}
```
## Configurando múltiplos databases

Múltiplos databases podem ser utilizados em sua aplição, bastando criar cada uma das configurações de acesso e instanciando o database com uma delas, exemplo:

``` javascript
{
  ...
  'database1': { /*Configuração do database1*/
    'driverClassName': /*String Class do driver de conexão*/,
    ...
  },
  'database2': { /*Configuração do database2*/
    'driverClassName': /*String Class do driver de conexão*/,
    ...
  }
}
```
``` javascript
let dbm = require('database')

let dbConfig1 = getBitcodeConfig('database1')()
let db = dbm.createDbInstance(dbConfig1)

let dbConfig2 = getBitcodeConfig('database2')()
let db = dbm.createDbInstance(dbConfig2)
```
