
function show() {
    var args = Array.prototype.slice.call(arguments).map(function(arg) {
        return (arg.constructor && (arg.constructor.toString().contains('function Array()') || arg.constructor.toString().contains('function Object()')))
                ? JSON.stringify(arg)
                : arg
    })

    print.apply(null, args)
}


print("loading utils.js ...............................................")

exports = {
    nome: "Thrust",
    ano: 2018
}

var lixo = "lixo"

print(lixo)

lixo = 5
