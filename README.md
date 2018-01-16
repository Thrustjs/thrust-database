# thrust-database

Módulo de acesso a dados em Banco de Dados Relacional (SQL DB)


# Tutorial

Para utilizar as APIs de acesso a banco de dados é necessário importar o bitcode "database"
```javascript
    let dbm = require("database")
```

Defina as opções do pool de conexões. Uma boa prática é colocar esta configuração no arquivo config.json que 
fica no diretório raiz da aplicação
```javascript
    let dbConfig = getBitcodeConfig("database")()
```

Depois execute a função de criação do objeto que disponibilizará as APIs de banco de dados
```javascript
    let db = dbm.createDbInstance(dbConfig)
```

Agora ficou fácil. É só utilizar os métodos de acesso à dados para construir sua aplicação
```javascript
    let rs
    
    rs = db.execute("DROP TABLE IF EXISTS ttest")

    if (rs.error === false) {
        rs = db.execute("CREATE TABLE ttest (key INTEGER PRIMARY KEY AUTOINCREMENT, num NUMERIC, txt VARCHAR(64)) ")

        if (rs.error === false) {
            rs = db.execute("INSERT INTO ttest (num, txt) values (1, 'Num Um')", true)
            show("Array de chaves dos registros inseridos: ", rs.keys)

            let regs = [{ num: 10, txt: "Num Dez" }, { num: 11, txt: "Num Onze" }, { num: 12, txt: "Num Doze" }]

            rs = db.insert("ttest", regs)
            show("Array de chaves dos registros inseridos: ", rs.keys)

            rs = db.execute("SELECT * FROM ttest WHERE num = :numero AND txt = :texto", {numero: 11, texto: "Num Onze"})
            show("Result =>", rs)

            rs = db.select("SELECT * FROM ttest WHERE num = :numero AND txt = :texto", {numero: 10, texto: "Num Dez"})
            show("Result =>", rs)
        }
    }

```


