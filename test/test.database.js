/**
 * @author Nery Jr
 */
var rdbms = 'postgresql'
var cfgDatabase = getBitcodeConfig('database')()
var dbConfig = cfgDatabase[rdbms]

dbConfig.dialect = rdbms

var sqls = {
  sqlite: {
    create: 'CREATE TABLE ttest (id INTEGER PRIMARY KEY AUTOINCREMENT, num NUMERIC, txt VARCHAR(64), dat TEXT)'
  },
  javadb: {
    create: 'CREATE TABLE ttest (id INT not null primary key GENERATED ALWAYS AS IDENTITY, num NUMERIC, txt VARCHAR(64), dat DATE)'
  },
  h2: {
    create: 'CREATE TABLE "ttest" ("id" BIGINT AUTO_INCREMENT, "num" NUMERIC, "txt" VARCHAR(64), "dat" TEXT)'
  },
  postgresql: {
    create: 'CREATE TABLE "ttest" ("id" BIGSERIAL PRIMARY KEY, "num" NUMERIC, "txt" VARCHAR(64), "dat" TEXT)'
  }
}

function log(user, dbFunctionName, statementMethodName, sql) {
  var d = new Date()

  print(d.getFullYear() + '/' + (d.getMonth() + 1) + '/' + d.getDate(), '|',
    d.getHours() + ':' + d.getMinutes() + ':' + d.getSeconds() + '.' + d.getMilliseconds(), '|',
    user, '|', dbFunctionName, '|', statementMethodName, '|',
    '[' + sql + ']'
  )
}

// dbConfig.logFunction = log.bind(null, "Nery")
dbConfig.returnColumnLabel = false

var db = require('database').createDbInstance(dbConfig)
var majesty = require('majesty')

function exec(describe, it, beforeEach, afterEach, expect, should, assert) {
  var rs

  // afterEach(function() { })
  // beforeEach(function() { })

  describe('Módulo de acesso a base de dados relacional [db]', function() {
    describe('API [execute]', function() {
      it('Executando comando DML DROP TABLE table', function() {
        // try { db.execute('DROP TABLE ttest') } catch (e) {}
        rs = db.execute('DROP TABLE IF EXISTS "ttest"')
        expect(rs.error).to.equal(false)
      })

      it('Executando comando DML CREATE table', function() {
        expect(db.execute(sqls[rdbms].create).error).to.equal(false)
      })

      it('Executando comando INSERT table', function() {
        expect((rs = db.execute('INSERT INTO "ttest" ("num", "txt") values (1, \'Num Um\')', null, true)).error).to.equal(false)
        expect(rs.keys.length).to.equal(1)
        expect((rs = db.execute('INSERT INTO "ttest" ("num", "txt") values (2, \'Num Dois\'), (3, \'Num Três\')', {}, true)).error).to.equal(false)
        expect(rs.keys.length).to.above(0)
        expect(db.execute('SELECT * FROM "ttest"').length).to.equal(3)

        expect(db.execute('INSERT INTO "ttest" ("num", "txt") values (4, :num)', { num: 'Num Quatro' }, true).keys.length).to.equal(1)
        expect(db.execute('SELECT * FROM "ttest"').length).to.equal(4)

        expect(db.execute('INSERT INTO "ttest" ("num", "txt") values (5, \'Num Cinco\'), (6, \'Num Seis\'), (7, \'Num Sete\')').error).to.equal(false)
        expect(db.execute('SELECT * FROM "ttest"').length).to.equal(7)
      })

      it("Executando comando UPDATE table sem 'bind' de parâmetros", function() {
        expect((rs = db.execute('UPDATE "ttest" SET "num" = "num" * 10')).error).to.equal(false)
        expect(rs.affectedRows).to.equal(7)
        expect((rs = db.execute('UPDATE "ttest" SET "num" = "num" * 10, "txt"=\'Trezentos\' WHERE "num"=30')).error).to.equal(false)
        expect(rs.affectedRows).to.equal(1)
        // expect(db.execute('SELECT num, txt FROM "ttest" WHERE num=300')[0])
        //   .to.include({ num: 300, txt: 'Trezentos' })
        expect(db.execute('SELECT "num", "txt" FROM "ttest" WHERE "num"=300')).to.satisfy(function(rs) {
          return rs && rs.length === 1 && parseInt(rs[0].num) === 300 && rs[0].txt === 'Trezentos'
        })
        expect((rs = db.execute('UPDATE "ttest" SET "num"="num"/10, "txt"=\'Num Três\' WHERE "num"=300')).error).to.equal(false)
        expect(rs.affectedRows).to.equal(1)
        expect((rs = db.execute('UPDATE "ttest" SET "num"="num"/10')).error).to.equal(false)
        expect(rs.affectedRows).to.equal(7)
      })

      it("Executando comando UPDATE table com 'bind' de parâmetros", function() {
        expect(db.execute('UPDATE "ttest" SET "num" = "num" * :value', { value: 10 }).affectedRows).to.equal(7)
        expect(db.execute('UPDATE "ttest" SET "num" = "num" / :value', { value: 10 }).affectedRows).to.equal(7)
      })

      it("Executando comando DELETE table sem 'bind' de parâmetros", function() {
        expect((rs = db.execute('DELETE FROM "ttest" WHERE "num"=1 OR "id"=5')).error).to.equal(false)
        expect(rs.affectedRows).to.equal(2)

        expect((rs = db.execute('DELETE FROM "ttest" WHERE "num" IN (2, 4)')).error).to.equal(false)
        expect(rs.affectedRows).to.equal(2)

        expect(db.execute('SELECT COUNT(*) as "count" FROM "ttest"')[0].count).to.equal(3)
        expect(parseInt(db.execute('SELECT * FROM "ttest" WHERE "num"=3')[0].num)).to.equal(3)
      })

      it("Executando DELETE table com 'bind' de parâmetros", function() {
        expect(db.execute('DELETE FROM "ttest" WHERE "num" = :num OR "id" = :id', { num: 3, id: 20 }).affectedRows).to.equal(1)

        expect(db.execute('INSERT INTO "ttest" ("num", "txt") values (8, \'Num Oito\')').error).to.equal(false)
        expect(db.execute('DELETE FROM "ttest" WHERE "num" = :num OR "id" = :id', { num: 8, id: 7 }).affectedRows).to.equal(2)

        expect((rs = db.execute('DELETE FROM "ttest"')).error).to.equal(false)
        // show("Select * => ", db.execute("SELECT * FROM "ttest""))
      })

      it('Executando comando SELECT table', function() {
        db.execute('INSERT INTO "ttest" ("num", "txt") values (1, \'Num Um\'), ' +
          ' (2, \'Num Dois\'), (3, \'Num Três\'), (4, \'Num Quatro\'), (5, \'Num Cinco\')')

        rs = db.execute('SELECT * FROM "ttest"')
        expect(rs.length).to.equal(5)
        expect(rs.constructor.name).to.equal('Array')

        rs = db.execute('SELECT * FROM "ttest" WHERE "num" = :numero AND "txt" = :texto', { numero: 5, texto: 'Num Cinco' })
        expect(rs.length).to.equal(1)

        rs = db.select('SELECT * FROM "ttest" WHERE "num" = :numero AND "txt" = :texto', { numero: 2, texto: 'Num Dois' })
        expect(rs.length).to.equal(1)

        rs = db.select('ttest', [], { num: 2, txt: 'Num Dois' })
        // print('rs =>', JSON.stringify(rs))
        expect(rs).to.satisfy(function(rs) {
          return rs && rs.length === 1 && parseInt(rs[0].num) === 2 && rs[0].txt === 'Num Dois'
        })

        rs = db.select('ttest', ['num'], { num: 2, txt: 'Num Dois' })
        // print('rs =>', JSON.stringify(rs))
        expect(rs).to.satisfy(function(rs) {
          return rs && rs.length === 1 && parseInt(rs[0].num) === 2 && rs[0].txt === undefined
        })

        rs = db.select('ttest', ['num'])
        // print('\nrs =>', JSON.stringify(rs))
        expect(rs.length).to.be.above(1)
        expect(rs.length).to.be.equal(5)

        rs = db.select('ttest')
        // print('\nrs =>', JSON.stringify(rs))
        expect(rs.length).to.be.above(1)
        expect(rs.length).to.be.equal(5)

        // expect((rs = db.execute("SELECT COUNT(*) as count FROM "ttest"")).length).to.equal(1)
        // expect(rs[0].count).to.equal(5)
        // expect((rs = db.execute("SELECT num, txt FROM "ttest" ORDER BY num LIMIT 2")).length).to.equal(2)
        // expect(JSON.stringify(rs[0])).to.equal(JSON.stringify({num: 1, txt: "Num Um"}))
        // expect(JSON.stringify(rs[1])).to.equal(JSON.stringify({num: 2, txt: "Num Dois"}))
        // expect((rs = db.execute("SELECT num, txt FROM "ttest" ORDER BY num LIMIT 2 OFFSET 2")).length).to.equal(2)
        // expect(JSON.stringify(rs[0])).to.equal(JSON.stringify({num: 3, txt: "Num Três"}))
        // expect(JSON.stringify(rs[1])).to.equal(JSON.stringify({num: 4, txt: "Num Quatro"}))
        // expect((rs = db.execute("SELECT num, txt FROM "ttest" ORDER BY num LIMIT 2 OFFSET 4")).length).to.equal(1)
        // expect(JSON.stringify(rs)).to.equal(JSON.stringify([{num: 5, txt: "Num Cinco"}]))
        // expect((rs = db.execute("SELECT num, txt FROM "ttest" WHERE num > 1 ORDER BY num LIMIT 2 OFFSET 2")).length).to.equal(2)
        // expect(JSON.stringify(rs)).to.equal(JSON.stringify([{"num":4,"txt":"Num Quatro"},{"num":5,"txt":"Num Cinco"}]))
      })

      it('Executando SELECT COUNT(*) table', function() {
        rs = db.execute('SELECT COUNT(*) AS "total" FROM "ttest"')
        expect(rs.length).to.equal(1)
        expect(rs.constructor.name).to.equal('Array')
        console.log('rs =>', rs)

        expect(rs[0]).to.satisfy(function(rs) {
          return rs && ((rs.total === 5 && dbConfig.returnColumnLabel === true) ||
            (dbConfig.returnColumnLabel === false && rs['total'] === 5))
        })
      })
    })

    describe('API [insert]', function() {
      it('Apagando todos os registros da tabela DELETE table', function() {
        expect(db.execute('DELETE FROM "ttest"').error).to.equal(false)
      })

      it('Inserindo um registro por comando na tabela', function() {
        rs = db.insert('ttest', { num: 1, txt: 'Num Um' })
        expect(rs.error).to.equal(false)
        expect(rs.affectedRows).to.equal(1)

        expect((rs = db.insert('ttest', { txt: 'Num Dois e [num] = null' }, true)).error).to.equal(false)
        expect(rs.keys.length).to.equal(1)

        expect((rs = db.insert('ttest', { num: 3 }, true)).error).to.equal(false)
        expect(rs.keys.length).to.equal(1)

        expect((rs = db.execute('SELECT * FROM "ttest"')).constructor.name).to.equal('Array')
        expect(rs.length).to.equal(3)
      })

      it('Inserindo vários registro por comando na tabela', function() {
        var regs = [{ num: 10, txt: 'Num Dez' }, { num: 11, txt: 'Num Onze' }, { num: 12, txt: 'Num Doze' }]

        expect(db.insert('ttest', regs).error).to.equal(false)
        // expect(rs.keys.length).to.above(0)
        expect(db.execute('SELECT * FROM "ttest"').length).to.equal(6)

        regs = [{ num: 13 }, { txt: 'Num Quatorze' }, { num: 15, txt: 'Num Quinze' }]

        expect(db.insert('ttest', regs).error).to.equal(false)
        expect(db.execute('SELECT * FROM "ttest"').length).to.equal(9)
      })
    })

    describe('API [update]', function() {
      it('Alterando registro(s) da tabela UPDATE table (com where)', function() {
        // expect(db.update('ttest', { num: 100, txt: 'Num Cem' }, { num: 10 }).error).to.equal(false)
        rs = db.update('ttest', { num: 100, txt: 'Num Cem' }, { num: 10 })
        expect(rs.error).to.equal(false)
        expect(rs.affectedRows).to.equal(1)
        expect(db.execute('SELECT "num", "txt" FROM "ttest" WHERE "num"=100')).to.satisfy(function(rs) {
          return rs && rs.length === 1 && parseInt(rs[0].num) === 100 && rs[0].txt === 'Num Cem'
        })

        expect(db.update('ttest', { num: 100, txt: null }, { num: 100 }).error).to.equal(false)
        expect(db.execute('SELECT "num", "txt" FROM "ttest" WHERE "num"=100')).to.satisfy(function(rs) {
          return rs && rs.length === 1 && parseInt(rs[0].num) === 100 && rs[0].txt === null
        })

        expect(db.update('ttest', { num: 10, txt: 'Num Dez' }, { num: 100 }).error).to.equal(false)
        expect(db.execute('SELECT "num", "txt" FROM "ttest" WHERE "num"=10')).to.satisfy(function(rs) {
          return rs && rs.length === 1 && parseInt(rs[0].num) === 10 && rs[0].txt === 'Num Dez'
        })
      })

      it('Alterando todos os registros da tabela UPDATE table (sem where)', function() {
        expect(db.update('ttest', { num: 777 }).error).to.equal(false)
        expect(db.execute('SELECT * FROM "ttest" WHERE "num"=777').length).to.equal(9)
      })
    })

    describe('API [delete]', function() {
      it('Apagando todos os registros da tabela DELETE table (sem where)', function() {
        expect(db.delete('ttest').error).to.equal(false)
        expect(db.execute('SELECT * FROM "ttest"').length).to.equal(0)
      })

      it('Inserindo 3 novos registros na table com api [insert]', function() {
        var regs = [{ num: 10, txt: 'Num Dez' }, { num: 11, txt: 'Num Onze' }, { num: 12, txt: 'Num Doze' }]

        expect(db.insert('ttest', regs).error).to.equal(false)
        expect(db.execute('SELECT * FROM "ttest"').length).to.equal(3)
      })

      it('Inserindo registro com valor nulo na table com api [insert]', function() {
        let regs = [{ num: 16, txt: null }]
        expect(db.insert('ttest', regs).error).to.equal(false)
      })

      it('Apagando registro(s) da tabela DELETE table (com where)', function() {
        expect(db.delete('ttest', { num: 11 }).error).to.equal(false)
        expect(db.delete('ttest', { num: 16 }).error).to.equal(false)
        expect(db.execute('SELECT * FROM "ttest"').length).to.equal(2)
      })
    })

    describe('API [executeInSingleTransaction] -  execução de commandos SQL em uma única transação', function() {
      it('Executando sequência de comandos SQL em um cenário de NÃO problemas ou erro (commit) ', function() {
        rs = db.executeInSingleTransaction(function(db, context) {
          var cmd = 'INSERT INTO "ttest" ("num", "txt") values (6, \'Num Seis\'), ' +
            " (7, 'Num Sete'), (8, 'Num Oito'), (9, 'Num Nove')"

          db.execute(cmd)
          db.execute('UPDATE "ttest" SET "num" = :num, "txt" = :txt WHERE "num"=9', context)
        }, { num: 99, txt: 'Num Noventa e Nove' })

        expect(rs.error).to.equal(false)
        expect(db.execute('SELECT COUNT(*) as "count" FROM "ttest" WHERE "num"=99').length).to.equal(1)
        // show(db.execute('SELECT COUNT(*) as count FROM "ttest" WHERE "num"=99'))
        expect(db.execute('SELECT COUNT(*) as "count" FROM "ttest" WHERE "num"=99')[0].count).to.equal(1)
      })

      it('Executando transação (sequência de comandos SQL) em um cenário COM problemas ou erro (rollback) ', function() {
        // testando exeções e rollback
        rs = db.executeInSingleTransaction(function(db, context) {
          rs = db.execute('UPDATE "ttest" SET "num"=' + context.num + ', "txt" = \'' + context.txt + '\' WHERE "num"=99')

          if (true)
            throw { error: true }

          rs = db.execute('DELETE FROM "ttest"')
        }, { num: 999, txt: 'Num Novecenetos e Noventa e Nove' })

        expect(rs.error).to.equal(true)
        expect((rs = db.execute('SELECT COUNT(*) as "count" FROM "ttest" WHERE "num"=99')).length).to.equal(1)
        expect(rs[0].count).to.equal(1)
      })
    })

    describe('Prevenção de [SQL Inject]', function() {
      it('Executando comando DML DROP TABLE table', function() {
        rs = db.execute('DROP TABLE IF EXISTS "ttest"')
        expect(rs.error).to.equal(false)
      })

      it('Executando comando DML CREATE table', function() {
        expect(db.execute(sqls[rdbms].create).error).to.equal(false)
      })

      it('Utilizando API [execute]', function() {
        var sqlInject = "'Num Um'); INSERT INTO ttest (2, 'Num Dois'"
        expect(db.execute('INSERT INTO "ttest" ("num", "txt") values (1, :num)', { num: sqlInject }, true).keys.length).to.equal(1)
        expect(db.execute('SELECT * FROM "ttest"').length).to.equal(1)

        rs = db.execute('INSERT INTO "ttest" ("num", "txt") values (2, \'Num Dois\');\n ' +
          'INSERT INTO "ttest" ("num", "txt") values (3, \'Num Tres\')')
        // console.log('\nrs =>', rs)
        // console.log('\nselect * =>', db.execute('SELECT * FROM "ttest"'))

        expect(rs.affectedRows).to.equal(1)
        expect(db.execute('SELECT * FROM "ttest"').length).to.above(1)
        // .to.satisfy(function(rs) {
        //   // não insere as tuplas: erro de SQL Injection
        //   return rs.error === true && rs.message === 'Attempt sql injection!'
        // })
      })

      it('Utilizando API [insert]', function() {
        var descricao = "'); DROP TABLE \"ttest\"; SELECT ('ttest"

        db.execute('DELETE FROM "ttest"')

        db.insert('ttest', { id: 1, txt: descricao })
        rs = db.execute('SELECT * FROM "ttest"')
        expect(rs.length).to.be.lessThan(2)
        expect(rs[0].txt.length).to.be.equal(38)
      })

      it('Utilizando API [update]', function() {
        var novoValor = "Num Cem'); DELETE TABLE \"ttest\"; SELECT ('ttest"

        db.execute('DELETE FROM "ttest"')
        rs = db.insert('ttest', { id: 1, num: 1, txt: 'Num Um' })

        rs = db.update('ttest', { txt: novoValor }, { num: 1 })
        expect(rs.affectedRows).to.be.equal(1)
        expect(db.execute('SELECT * FROM "ttest"').length).to.equal(1)
      })
    })

    describe('Binding Array', function() {
      it('Apagando todos os registros (Delete table)', function() {
        // expect(db.execute('DELETE FROM "ttest"').error).to.be.equal(false)
        expect(db.execute('DROP TABLE IF EXISTS "ttest"').error).to.be.equal(false)
        expect(db.execute(sqls[rdbms].create).error).to.equal(false)
      })

      it('Inserindo 3 novos registros', function() {
        var regs = [{ num: 10, txt: 'Num Dez' }, { num: 11, txt: 'Num Onze' }, { num: 12, txt: 'Num Doze' }]

        expect(db.insert('ttest', regs).error).to.equal(false)
        expect(rs.error).to.be.equal(false)
      })

      it('API [select]', function() {
        rs = db.select('SELECT * FROM "ttest" WHERE "num" IN (:numeros)', { numeros: [11, 12] })
        expect(rs.length).to.equal(2)
      })

      it('API [execute] com um IN', function() {
        rs = db.execute('UPDATE "ttest" SET "num" = "num" * :value WHERE "num" IN (:numeros)', { value: 10, numeros: [11, 12] })
        expect(rs.affectedRows).to.equal(2)

        rs = db.select('SELECT * FROM "ttest" WHERE "num" IN (:numeros)', { numeros: [110, 120] })
        expect(rs.length).to.equal(2)

        rs = db.select('SELECT * FROM "ttest" WHERE "num" IN (110, 120)')
        expect(rs.length).to.equal(2)
      })

      it('API [execute] com vários IN', function() {
        // console.log('\nrs =>', db.execute('SELECT * FROM ttest'))
        rs = db.execute('SELECT * FROM "ttest" WHERE "id" IN (:ids) AND "num" IN (:nums)', {
          ids: [1, 2, 3, 4, 5],
          nums: [110, 120]
        })
        expect(rs.length).to.equal(2)
      })
    })
  })
}

var res = majesty.run(exec)

print('', res.success.length, ' scenarios executed with success and')
print('', res.failure.length, ' scenarios executed with failure.\n')

res.failure.forEach(function(fail) {
  print('[' + fail.scenario + '] =>', fail.execption)
  var i = 0
  if (fail.execption.printStackTrace && i === 0) {
    fail.execption.printStackTrace()
    i++
  }
})

// java.lang.Runtime.getRuntime().exec("cmd /k chcp 65001");
