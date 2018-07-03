Database
[![Build Status](https://travis-ci.org/thrust-bitcodes/database.svg?branch=master)](https://travis-ci.org/thrust-bitcodes/database) [![GitHub release](https://img.shields.io/github/release/thrust-bitcodes/database.svg)](https://github.com/thrust-bitcodes/database/releases)
===============

Database é um *bitcode* de acesso a dados em Bando de Dados Relacional (SQL DB) para [thrust](https://github.com/thrustjs/thrust).

#Importante

A partir da versão `0.2.28` o database funciona apenas com o thrust `0.5.0` e acima.
Para utilizar o database em versões anteriores do thrust, utilize no máximo a versão `0.2.27`

# Instalação

Posicionado em um app [thrust](https://github.com/thrustjs/thrust), no seu terminal:

```bash
thrust install database
```

# Tutorial

Para utilizar as APIs de acesso a banco de dados é necessário importar o bitcode 'database'
```javascript
    var dbm = require('database')
```

Defina as opções do pool de conexões. Uma boa prática é colocar esta configuração no arquivo config.json que
fica no diretório raiz da aplicação
```javascript
    var dbConfig = getConfig().database
```

Depois execute a função de criação do objeto que disponibilizará as APIs de banco de dados
```javascript
    var db = dbm.createDbInstance(dbConfig)
```

Agora ficou fácil. É só utilizar os métodos de acesso à dados para construir sua aplicação
```javascript
    var rs

    rs = db.execute('DROP TABLE IF EXISTS "ttest"')

    if (rs.error === false) {
        rs = db.execute('CREATE TABLE "ttest" ("key" INTEGER PRIMARY KEY AUTOINCREMENT, "num" NUMERIC, "txt" VARCHAR(64)) ')

        if (rs.error === false) {
            rs = db.execute('INSERT INTO "ttest" ("num", "txt") values (1, 'Num Um')', true)
            show('Array de chaves dos registros inseridos: ', rs.keys)

            var regs = [{ num: 10, txt: 'Num Dez' }, { num: 11, txt: 'Num Onze' }, { num: 12, txt: 'Num Doze' }]

            rs = db.insert('ttest', regs)
            show('Array de chaves dos registros inseridos: ', rs.keys)

            rs = db.execute('SELECT * FROM "ttest" WHERE "num" = :numero AND "txt" = :texto', {numero: 11, texto: 'Num Onze'})
            show('Result =>', rs)

            rs = db.select('SELECT * FROM "ttest" WHERE "num" = :numero AND "txt" = :texto', {numero: 10, texto: 'Num Dez'})
            show('Result =>', rs)

            // executando vários comandos dentro de uma mesma transação
            rs = db.executeInSingleTransaction(function (db, context) {
                var cmd = 'INSERT INTO "ttest" ("num", "txt") values (6, \'Num Seis\'), ' +
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
    'dateAsString': /*Boolean Determina se campos do tipo data serão considerados como string (Default: false) */

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
var dbm = require('database')

var dbConfig1 = getBitcodeConfig('database1')()
var db = dbm.createDbInstance(dbConfig1)

var dbConfig2 = getBitcodeConfig('database2')()
var db = dbm.createDbInstance(dbConfig2)
```

## What's new

v0.2.24 - Fix: Caso fosse feito  um select em uma coluna do tipo texto, e o valor fosse um número
o database convertia o mesmo para número, ajustado para que seja retornada a string.

v0.2.23 - Melhoria: possibilidade de retorno do _column label_ ao invés do _column name_ no retorno dos SELECTs
* Configuração através do _options_._returnColumnLabel_ no [createDbInstance] o retorno do array de JSON como nome do alias e não da coluna

v0.2.22 - Fix: método [createDbInstance] ao selecionar o _dialect_ SQL
* Correção ao selecionar o _dialect_ SQL. Erro ocorria quando o dialeto era passado via options no
método [createDbInstance]

v0.2.21 - Fix: método [bindParams] para utilizar _arrays_ em mais de uma cláusula com IN
* Correção do método [bindParams] ao executar comandos SQL com mais de uma cláusula IN
* Adição de cenários de testes

v0.2.20 - Melhoria: métodos [select] e [execute] para utilizarem _array_
* Alteração dos métodos [select] e [execute] para utilizarem _array_ na cláusula IN
* Adição de cenários de testes

v0.2.18 - FIX: Ajustando validação do sqlSelect pra queries que iniciam com WITH
* Fix: corrigindo método sqlSelect / erro de atribuição na variável 'sql'.

v0.2.17 - FIX SELECT começando com WITH
* Fix: corrigindo método sqlSelect para aceitar comandos que iniciem com WITH.

v0.2.15 - Melhoria: Eliminando o uso de RegEx para previnir SQL Inject
* Eliminando o uso de RegEx para previnir SQL Inject nos comandos SQL.
* Eliminando concateções de strings na formação dos comandos SQL e utilização de _bind_ de parâmetros para todos os comandos.

v0.2.10 - Fix: Previnindo sql inject nas APIs [insert] e [update], corrigindo SQLs com WITH

v.0.2.9 - FIX: Correção/melhoria no método update
* Correção do método update, para atualizar campo com valor _nulo_
* Adição de cenários de testes
