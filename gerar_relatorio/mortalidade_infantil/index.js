const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');
const {LocalStorage} = require("node-localstorage")
const localStorage = new LocalStorage('./'); 


let caminho_csv_analisar = 'C:\\Users\\Mikael\\Documents\\projeto-integrador\\data\\csv_analisar\\mortalidade_infantil\\'
let caminho_csv_relatorio = 'C:\\Users\\Mikael\\Documents\\projeto-integrador\\data\\csv_relatorio\\mortalidade_infantil\\'


let totalCirurgiaSim = 0;
let totalCirurgiaNao = 0;
let totalCirurgiaIgnorada = 0;
let totalCirurgiaVazia = 0;
let totalLinhasInvalidas = 0;
var csvFinal = [['CIRURGIA']];

const csv_para_relatorio = [`DOINF10`,`DOINF11`, `DOINF12`, `DOINF13`, `DOINF14`, `DOINF15`, `DOINF16`, `DOINF17`, `DOINF18`, `DOINF19`, `DOINF20`];
    
csv_para_relatorio.forEach(nome_arquivo => {
    fs.createReadStream(path.resolve(`${caminho_csv_analisar}${nome_arquivo}.csv`))
    .pipe(csv.parse({ headers: true, ignoreEmpty: true  }))
    .validate((data) => data.CIRURGIA !== '')
    .on('error', (error) => console.error(error))
    .on('data', (row) => {contabilizarCirurgias(row), prepararObjeto(row)})
    .on('data-invalid', (row) => contabilizarInvalidos(row))
    .on('end', (rowCount) => {
        salvarCSV(csvFinal, nome_arquivo, caminho_csv_relatorio)
        salvarJSON(nome_arquivo, totalCirurgiaSim, totalCirurgiaNao, totalCirurgiaIgnorada, totalCirurgiaVazia, totalLinhasInvalidas, rowCount)
    });
});

function prepararObjeto(obito) {
    obito = [ obito.CIRURGIA]
    csvFinal.push(obito)
    return obito
}


function contabilizarCirurgias(obito) {
    if (obito.CIRURGIA == 1) {
        totalCirurgiaSim++
    }

    if (obito.CIRURGIA == 2) {
        totalCirurgiaNao++
    }

    if (obito.CIRURGIA == 9) {
        totalCirurgiaIgnorada++
    }
}

function contabilizarInvalidos(obitoInvalido) {
    totalLinhasInvalidas ++ 

    if(obitoInvalido.CIRURGIA == ''){
        totalCirurgiaVazia++
    } 
}

function salvarCSV(csvFinal, nome_arquivo, caminho_arquivo) {
    csv.writeToPath(path.resolve(`${caminho_arquivo}${nome_arquivo}-FINAL.csv`), csvFinal)
    .on('error', err => console.error(err))
    .on('finish', () => {console.log('Done writing.' + nome_arquivo)});
}

function salvarJSON(nome_arquivo, index, totalCirurgiaSim, totalCirurgiaNao, totalCirurgiaIgnorada, totalCirurgiaVazia,totalLinhasInvalidas, rowCount) {
    var relatorio_final = JSON.parse(localStorage.getItem('relatorio.json'))

    relatorio_final.push({
        [nome_arquivo]: {
            totalCirurgiaSim: totalCirurgiaSim,
            totalCirurgiaNao: totalCirurgiaNao,
            totalCirurgiaIgnorada: totalCirurgiaIgnorada,
            totalCirurgiaVazia: totalCirurgiaVazia,
            totalLinhasInvalidas: totalLinhasInvalidas,
            rowCount: rowCount
        }
    })

    localStorage.setItem('relatorio.json', JSON.stringify(relatorio_final, null, '\t'))
    
    // if ((estado_atual + 1) != estados.length) {
    //     estado_atual ++
    //     gerar_relatorio()
    // }
}