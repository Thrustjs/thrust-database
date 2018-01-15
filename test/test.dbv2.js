/**
 * @author Nery Jr
 */

function show() {
    var args = Array.prototype.slice.call(arguments).map(function(arg) {
        return (arg.constructor && (arg.constructor.name == "Array" || arg.constructor.name === "Object"))
            ? JSON.stringify(arg)
            : arg
    })

    print.apply(null, args)
}


loadJar("./sqlite-jdbc-3.21.0.1.jar")

let dbConfig = {        
    "driverClassName": "org.sqlite.JDBC",
    "urlConnection": "jdbc:sqlite:test.db",
    "userName": "",
    "password": ""
}


function log(user, dbFunctionName, statementMethodName, sql) {
    let d = new Date()

    print(d.getFullYear() + "/" + (d.getMonth()+1) + "/" + d.getDate(), "|",
        d.getHours() + ":" + d.getMinutes() + ":" + d.getSeconds() + "." + d.getMilliseconds(), "|",
        user, "|", dbFunctionName, "|", statementMethodName, "|",
        "[" + sql + "]"
    )
}

dbConfig.logFunction = log.bind(null, "Nery")

let db = require("../dist/dbv2").createDbInstance(dbConfig)
let majesty = require("majesty")


function exec(describe, it, beforeEach, afterEach, expect, should, assert) {
    var rs
    
    // afterEach(function() { })
    // beforeEach(function() { })

    describe("Módulo de acesso a base de dados relacional [db]", function () {

        describe("API de execução simples e direta de um commando SQL [execute]", function () {

            it("Executando comando DML DROP TABLE table", function () {
                rs = db.execute("DROP TABLE IF EXISTS ttest")
                expect(rs.error).to.equal(false)
            })

            it("Executando comando DML CREATE table", function () {
                rs = db.execute("CREATE TABLE ttest (key INTEGER PRIMARY KEY AUTOINCREMENT, num NUMERIC, txt VARCHAR(64)) ")
                expect(rs.error).to.equal(false)
            })

            it("Executando comando INSERT table", function () {
                expect((rs = db.execute("INSERT INTO ttest (num, txt) values (1, 'Num Um')", true)).error).to.equal(false)
                expect(rs.keys.length).to.equal(1)

                expect((rs = db.execute("INSERT INTO ttest (num, txt) values (2, 'Num Dois'), (3, 'Num Três')", true)).error).to.equal(false)
                expect(rs.keys.length).to.above(0)

                expect(db.execute("SELECT * FROM ttest").length).to.equal(3)
                
                expect(db.execute("INSERT INTO ttest (num, txt) values (4, 'Num Quatro'); "
                    + "INSERT INTO ttest (5, 'Num Cinco');")).to.satisfy(function(rs) {
                        // não insere a tupla {num: 5, txt: "Num Cinco"}
                        return rs.error === false && rs.keys[0] === 4
                    })
                expect(db.execute("SELECT * FROM ttest").length).to.equal(4)

                expect(db.execute("INSERT INTO ttest (num, txt) values (5, 'Num Cinco');\n "
                    + "INSERT INTO ttest (6, 'Num Seis');")).to.satisfy(function(rs) {
                        // não insere as tuplas: erro de SQL Injection
                        return rs.error === true && rs.message === "Attempt sql injection!"
                    })
                expect(db.execute("SELECT * FROM ttest").length).to.equal(4)

                expect(db.execute("INSERT INTO ttest (num, txt) values (5, 'Num Cinco')").error).to.equal(false)
                expect(db.execute("SELECT * FROM ttest").length).to.equal(5)
            })

            it("Executando comando UPDATE table ", function () {
                expect((rs = db.execute("UPDATE ttest SET num=num*10")).error).to.equal(false)
                expect(rs.affectedRows).to.equal(5)
                expect((rs = db.execute("UPDATE ttest SET num=num*10, txt='Trezentos' WHERE num=30")).error).to.equal(false)
                expect(rs.affectedRows).to.equal(1)
                expect(db.execute("SELECT num, txt FROM ttest WHERE num=300")[0])
                    .to.include({num: 300, txt: "Trezentos"})
                expect(db.execute("SELECT num, txt FROM ttest WHERE num=300")).to.satisfy(function(rs) {
                    return rs && rs.length === 1 && rs[0].num === 300 && rs[0].txt === "Trezentos"
                })
                expect((rs = db.execute("UPDATE ttest SET num=num/10, txt='Num Três' WHERE num=300")).error).to.equal(false)
                expect(rs.affectedRows).to.equal(1)
                expect((rs = db.execute("UPDATE ttest SET num=num/10")).error).to.equal(false)
                expect(rs.affectedRows).to.equal(5)
            })

            it("Executando comando DELETE table ", function () {
                expect((rs = db.execute("DELETE FROM ttest WHERE num=1 OR key=5")).error).to.equal(false)
                expect(rs.affectedRows).to.equal(2)
                expect((rs = db.execute("DELETE FROM ttest WHERE num IN (2, 4)")).error).to.equal(false)
                expect(rs.affectedRows).to.equal(2)
                expect(db.execute("SELECT COUNT(*) as count FROM ttest")[0].count).to.equal(1)
                expect(db.execute("SELECT * FROM ttest WHERE num=3")[0].num).to.equal(3)
                expect((rs = db.execute("DELETE FROM ttest")).error).to.equal(false)
                // show("Select * => ", db.execute("SELECT * FROM ttest"))
            })

            it("Executando comando SELECT table", function () {
                db.execute("INSERT INTO ttest (num, txt) values (1, 'Num Um'), " 
                    + " (2, 'Num Dois'), (3, 'Num Três'), (4, 'Num Quatro'), (5, 'Num Cinco')")

                rs = db.execute("SELECT * FROM ttest")

                expect(rs.length).to.equal(5)
                expect(rs.constructor.name).to.equal("Array")
                // print(JSON.stringify(rs))

                // expect((rs = db.execute("SELECT COUNT(*) as count FROM ttest")).length).to.equal(1)
                // expect(rs[0].count).to.equal(5)
                // expect((rs = db.execute("SELECT num, txt FROM ttest ORDER BY num LIMIT 2")).length).to.equal(2)
                // expect(JSON.stringify(rs[0])).to.equal(JSON.stringify({num: 1, txt: "Num Um"}))
                // expect(JSON.stringify(rs[1])).to.equal(JSON.stringify({num: 2, txt: "Num Dois"}))
                // expect((rs = db.execute("SELECT num, txt FROM ttest ORDER BY num LIMIT 2 OFFSET 2")).length).to.equal(2)
                // expect(JSON.stringify(rs[0])).to.equal(JSON.stringify({num: 3, txt: "Num Três"}))
                // expect(JSON.stringify(rs[1])).to.equal(JSON.stringify({num: 4, txt: "Num Quatro"}))
                // expect((rs = db.execute("SELECT num, txt FROM ttest ORDER BY num LIMIT 2 OFFSET 4")).length).to.equal(1)
                // expect(JSON.stringify(rs)).to.equal(JSON.stringify([{num: 5, txt: "Num Cinco"}]))
                // expect((rs = db.execute("SELECT num, txt FROM ttest WHERE num > 1 ORDER BY num LIMIT 2 OFFSET 2")).length).to.equal(2)
                // expect(JSON.stringify(rs)).to.equal(JSON.stringify([{"num":4,"txt":"Num Quatro"},{"num":5,"txt":"Num Cinco"}]))
            })
        })

        describe("API de execução simples e direta de um commando SQL [insert]", function () {

            it("Apagando todos os registros da tabela DELETE table", function () {
                expect(db.execute("DELETE FROM ttest").error).to.equal(false)
            })

            it("Inserindo um registro por comando na tabela", function () {
                rs = db.insert("ttest", {num: 1, txt: "Num Um"})
                expect(rs.error).to.equal(false)
                expect(rs.affectedRows).to.equal(1)

                expect((rs = db.insert("ttest", {txt: "Num Dois e [num] = null"})).error).to.equal(false)
                expect(rs.keys.length).to.equal(1)

                expect((rs = db.insert("ttest", {num: 3})).error).to.equal(false)
                expect(rs.keys.length).to.equal(1)

                expect((rs = db.execute("SELECT * FROM ttest")).constructor.name).to.equal("Array")
                expect(rs.length).to.equal(3)
            })

            it("Inserindo vários registro por comando na tabela", function () {
                let regs = [{num: 10, txt: "Num Dez"}, {num: 11, txt: "Num Onze"}, {num: 12, txt: "Num Doze"}]

                rs = db.insert("ttest", regs)
                expect(rs.error).to.equal(false)
                expect(rs.keys.length).to.above(0)
                expect(db.execute("SELECT * FROM ttest").length).to.equal(6)

                regs = [{num: 13}, {txt: "Num Quatorze"}, {num: 15, txt: "Num Quinze"}]

                expect(db.insert("ttest", regs).error).to.equal(false)
                expect(db.execute("SELECT * FROM ttest").length).to.equal(9)
            })
        })

        describe("API de execução simples e direta de um commando SQL [update]", function () {

            it("Alterando registro(s) da tabela UPDATE table (com where)", function () {
                expect(db.update("ttest", {num: 100, txt: "Num Cem"}, {num: 10}).error).to.equal(false)
                expect(db.execute("SELECT num, txt FROM ttest WHERE num=100")).to.satisfy(function(rs) {
                    return rs && rs.length === 1 && rs[0].num === 100 && rs[0].txt === "Num Cem"
                })

                expect(db.update("ttest", {num: 10, txt: "Num Dez"}, {num: 100}).error).to.equal(false)
                expect(db.execute("SELECT num, txt FROM ttest WHERE num=10")).to.satisfy(function(rs) {
                    return rs && rs.length === 1 && rs[0].num === 10 && rs[0].txt === "Num Dez"
                })
            })

            it("Alterando todos os registros da tabela UPDATE table (sem where)", function () {
                expect(db.update("ttest", {num: 777}).error).to.equal(false)
                expect(db.execute("SELECT * FROM ttest WHERE num=777").length).to.equal(9)
            })
        })

        describe("API de execução simples e direta de um commando SQL [delete]", function () {

            it("Apagando todos os registros da tabela DELETE table (sem where)", function () {
                expect(db.delete("ttest").error).to.equal(false)
                expect(db.execute("SELECT * FROM ttest").length).to.equal(0)
            })

            it("Inserindo 3 novos registros na table com api [insert]", function () {
                let regs = [{num: 10, txt: "Num Dez"}, {num: 11, txt: "Num Onze"}, {num: 12, txt: "Num Doze"}]

                rs = db.insert("ttest", regs)
                expect(rs.error).to.equal(false)
                expect(rs.keys.length).to.above(0)
                expect(db.execute("SELECT * FROM ttest").length).to.equal(3)
            })

            it("Apagando registro(s) da tabela DELETE table (com where)", function () {
                expect(db.delete("ttest", {num: 11}).error).to.equal(false)
                expect(db.execute("SELECT * FROM ttest").length).to.equal(2)
            })

        })



/*
        describe("API de execução de commandos SQL em uma única transação [executeInSingleTransaction].", function () {
        
            it("Executando sequência de comandos SQL em um cenário de NÃO problemas ou erro (commit) ", function() {
                var cmd = "INSERT INTO ttest (num, txt) values (6, 'Num Seis'), " 
                    + " (7, 'Num Sete'), (8, 'Num Oito'), (9, 'Num Nove')"
                
                rs = db.executeInSingleTransaction(function(db, context) {
                    db.execute(cmd)
                    db.execute("UPDATE ttest SET num=" + context.num + ", txt = '" + context.txt + "' WHERE num=9")
                }, {num: 99, txt: "Num Noventa e Nove"})

                expect(rs.error).to.equal(false)
                expect((rs = db.execute("SELECT COUNT(*) as count FROM ttest WHERE num=99")).length).to.equal(1)
                expect(rs[0].count).to.equal(1)
            })

            it("Executando transação (sequência de comandos SQL) em um cenário COM problemas ou erro (rollback) ", function() {
                // testando exeções e rollback
                rs = db.executeInSingleTransaction(function(db, context) {
                    rs = db.execute("UPDATE ttest SET num=" + context.num + ", txt = '" + context.txt + "' WHERE num=99")
                    
                    if (true)
                        throw {error: true}

                    rs = db.execute("DELETE FROM ttest")

                }, {num: 999, txt: "Num Novecenetos e Noventa e Nove"})

                expect(rs.error).to.equal(true)
                expect((rs = db.execute("SELECT COUNT(*) as count FROM ttest WHERE num=99")).length).to.equal(1)
                expect(rs[0].count).to.equal(1)
            })
        })
*/
    })    
}

let res = majesty.run(exec)

print("", res.success.length, " scenarios executed with success and")
print("", res.failure.length, " scenarios executed with failure.\n")

res.failure.forEach(function(fail) {
    print("[" + fail.scenario + "] =>", fail.execption)
    if (fail.execption.printStackTrace)
        fail.execption.printStackTrace()
})

// java.lang.Runtime.getRuntime().exec("cmd /k chcp 65001");