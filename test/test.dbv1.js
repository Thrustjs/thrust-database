/**
 * @author Nery Jr
 */

loadJar("./sqlite-jdbc-3.21.0.1.jar")

// loadJar("./idb-3.26.jar")
// "driverClassName": "org.enhydra.instantdb.jdbc.idbDriver",
// "urlConnection": "jdbc:idb:./instantdb.properties",

var db = require("../dist/index")
var majesty = require("majesty")


function exec(describe, it, beforeEach, afterEach, expect, should, assert) {
    var rs
    
    // afterEach(function() { print("", String.fromCharCode(++letter)) })
    // beforeEach(function() { })

    describe("Módulo de acesso a base de dados relacional [db]", function () {

        describe("API de execução simples e direta de um commando SQL [execute].", function () {

            it("Executando comando DML DROP TABLE table ", function () {
                rs = db.execute("DROP TABLE ttest")
                expect(rs.error).to.equal(false)
            })

            it("Executando comando DML CREATE table ", function () {
                rs = db.execute("CREATE TABLE ttest (key INTEGER PRIMARY KEY AUTOINCREMENT, num NUMERIC, txt VARCHAR(64)) ")
                expect(rs.error).to.equal(false)
            })

            it("Executando comando INSERT table ", function () {
                expect((rs = db.execute("INSERT INTO ttest (num, txt) values (1, 'Num Um')")).error).to.equal(false)
                expect(rs.keys.length).to.equal(1)
                expect((rs = db.execute("INSERT INTO ttest (num, txt) values (2, 'Num Dois'), " 
                        + "(3, 'Num Três'), (4, 'Num Quatro'), (5, 'Num Cinco')")).error).to.equal(false)
                expect(rs.keys).to.satisfy(function(res) {
                    return res.constructor.toString().contains("function Array()") 
                            && (res.length === 4 || (res.length === 1 && res[0] === 5))
                })
                expect((rs = db.execute("INSERT INTO ttest (num, txt) values (2, 'Numero Dois (2)'), " 
                        + "(4, 'Numero Quatro (2)')")).error).to.equal(false)
                expect(rs.keys).to.satisfy(function(res) {
                    return res.constructor.toString().contains("function Array()") 
                            && (res.length === 2 || (res.length === 1 && res[0] === 7))
                })
            })

            it("Executando comando UPDATE table ", function () {
                expect((rs = db.execute("UPDATE ttest SET num=num*10")).error).to.equal(false)
                expect(rs.affectedRows).to.equal(7)
                expect((rs = db.execute("UPDATE ttest SET num=num*10, txt='Trezentos' WHERE num=30")).error).to.equal(false)
                expect(rs.affectedRows).to.equal(1)
                expect((rs = db.execute("SELECT num, txt FROM ttest WHERE num=300")).length).to.equal(1)
                expect(JSON.stringify(rs[0])).to.equal(JSON.stringify({num: 300, txt: "Trezentos"}))
            })

            it("Executando comando DELETE table ", function () {
                expect((rs = db.execute("DELETE FROM ttest WHERE num=10 OR key=5")).error).to.equal(false)
                expect(rs.affectedRows).to.equal(2)
                expect((rs = db.execute("DELETE FROM ttest WHERE num IN (20, 40)")).error).to.equal(false)
                expect(rs.affectedRows).to.equal(4)
                expect((rs = db.execute("SELECT COUNT(*) FROM ttest")).length).to.equal(1)
                expect((rs = db.execute("DELETE FROM ttest")).error).to.equal(false)
            })

            it("Executando comando SELECT table ", function () {
                rs = db.execute("INSERT INTO ttest (num, txt) values (1, 'Num Um'), " 
                    + " (2, 'Num Dois'), (3, 'Num Três'), (4, 'Num Quatro'), (5, 'Num Cinco')")

                expect((rs = db.execute("SELECT * FROM ttest")).length).to.equal(5)
                expect(rs.length).to.equal(5)
                expect((rs = db.execute("SELECT COUNT(*) as count FROM ttest")).length).to.equal(1)
                expect(rs[0].count).to.equal(5)
                expect((rs = db.execute("SELECT num, txt FROM ttest ORDER BY num LIMIT 2")).length).to.equal(2)
                expect(JSON.stringify(rs[0])).to.equal(JSON.stringify({num: 1, txt: "Num Um"}))
                expect(JSON.stringify(rs[1])).to.equal(JSON.stringify({num: 2, txt: "Num Dois"}))
                expect((rs = db.execute("SELECT num, txt FROM ttest ORDER BY num LIMIT 2 OFFSET 2")).length).to.equal(2)
                expect(JSON.stringify(rs[0])).to.equal(JSON.stringify({num: 3, txt: "Num Três"}))
                expect(JSON.stringify(rs[1])).to.equal(JSON.stringify({num: 4, txt: "Num Quatro"}))
                expect((rs = db.execute("SELECT num, txt FROM ttest ORDER BY num LIMIT 2 OFFSET 4")).length).to.equal(1)
                expect(JSON.stringify(rs)).to.equal(JSON.stringify([{num: 5, txt: "Num Cinco"}]))
                expect((rs = db.execute("SELECT num, txt FROM ttest WHERE num > 1 ORDER BY num LIMIT 2 OFFSET 2")).length).to.equal(2)
                expect(JSON.stringify(rs)).to.equal(JSON.stringify([{"num":4,"txt":"Num Quatro"},{"num":5,"txt":"Num Cinco"}]))
            })

        })

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

    })
}

java.lang.Runtime.getRuntime().exec("cmd /k chcp 65001")

/*
let html = "<html>\n<body>\n"

majesty.report = {
    startExecution: function() {
        html += "<H1>Majesty started</H1>"
    },

    executionFinished: function() {
        print("\nFIM!!\n")
    },

    startOfSuite: function(suite) {
        // print(Array(suite.level+1).join("    "), suite.description)
        html += "<p>" + Array(suite.level+1).join("&nbsp&nbsp&nbsp") + suite.description + "</p>\n"
    },

    endOfSuite: function(suite) {
    },

    scenarioExecuted: function(scenario) {
        let result = "" + "[" + ((scenario.passed) ? "success" : "error") + "]"

        // print(Array(scenario.level+1).join("    "), result, scenario.description)
        // html += Array(scenario.level+1).join("&nbsp&nbsp&nbsp") +  result +  scenario.description
    }
}
*/

var res = majesty.run(exec)

print("", res.success.length, " scenarios executed with success and")
print("", res.failure.length, " scenarios executed with failure.\n")

res.failure.forEach(function(fail) {
    print("[", fail.scenario, "] => ", fail.execption)
})

// html += "</body>\n</html>\n"
// print("\n", html, "\n")

// java.lang.Runtime.getRuntime().exec("cmd /k chcp 65001");