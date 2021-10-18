const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');
const {LocalStorage} = require("node-localstorage")
const localStorage = new LocalStorage('./'); 

const estados = ['SP']
const casaEstado = 3;

let estado_atual = 0

let caminho_csv_analisar = 'E:\\Projetos\\projeto-integrador\\data\\csv_analisar\\mortalidade_envolvendo_cirurgia\\Sudeste\\'
let caminho_csv_relatorio = 'E:\\Projetos\\projeto-integrador\\data\\csv_relatorio\\mortalidade_envolvendo_cirurgia\\Sudeste\\'

let totalDtIdadeVazia = 0;
let totalCirurgiaSim = 0;
let totalCirurgiaNao = 0;
let totalCirurgiaIgnorada = 0;
let totalCirurgiaVazia = 0;
let totalLinhasInvalidas = 0;
let totalGeralIdade = 0;
let totalMeiaIdade = 0;
let totalTerceiraIdade = 0
var csvFinal = [['IDADE', 'CIRURGIA']];

gerar_relatorio()

function gerar_relatorio() {
    const csv_para_relatorio = [`DO${estados[estado_atual]}2011`, `DO${estados[estado_atual]}2012`, `DO${estados[estado_atual]}2013`, `DO${estados[estado_atual]}2014`, `DO${estados[estado_atual]}2015`, `DO${estados[estado_atual]}2016`, `DO${estados[estado_atual]}2017`, `DO${estados[estado_atual]}2018`, `DO${estados[estado_atual]}2019`, `DO${estados[estado_atual]}2020`];
    
    csv_para_relatorio.forEach(nome_arquivo => {
        fs.createReadStream(path.resolve(`${caminho_csv_analisar}${estados[estado_atual]}\\${nome_arquivo}.csv`))
        .pipe(csv.parse({ headers: true, ignoreEmpty: true  }))
        .validate((data) => data.IDADE !== '' && data.CIRURGIA !== '')
        .on('error', (error) => console.error(error))
        .on('data', (row) => {contabilizarCirurgias(row), prepararObjeto(row)})
        .on('data-invalid', (row) => contabilizarInvalidos(row))
        .on('end', (rowCount) => {
            salvarCSV(csvFinal, nome_arquivo, caminho_csv_relatorio, estados[estado_atual])
            salvarJSON(nome_arquivo, estado_atual, totalDtIdadeVazia, totalCirurgiaSim, totalCirurgiaNao, totalCirurgiaIgnorada, totalCirurgiaVazia, totalLinhasInvalidas,totalGeralIdade, totalMeiaIdade, totalTerceiraIdade, rowCount)
        });
    });
}

function prepararObjeto(obito) {
    obito = [formatIdade(obito.IDADE), obito.CIRURGIA]
    csvFinal.push(obito)
    return obito
}

function formatIdade(idade) {
    idade = idade.split('')
    idade = `${idade[1]}${idade[2]}`

    totalGeralIdade++

    if (idade >= 31 && idade <= 59){
        totalMeiaIdade ++
    }

    if (idade >= 60){
        totalTerceiraIdade ++
    }
    return idade
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

    if(obitoInvalido.IDADE == ''){
        totalDtIdadeVazia++
    } 

    if(obitoInvalido.CIRURGIA == ''){
        totalCirurgiaVazia++
    } 
}

function salvarCSV(csvFinal, nome_arquivo, caminho_arquivo, estado) {
    csv.writeToPath(path.resolve(`${caminho_arquivo}${estado}\\${nome_arquivo}-FINAL.csv`), csvFinal)
    .on('error', err => console.error(err))
    .on('finish', () => {console.log('Done writing.' + nome_arquivo)});
}

function salvarJSON(nome_arquivo, index, totalDtIdadeVazia, totalCirurgiaSim, totalCirurgiaNao, totalCirurgiaIgnorada, totalCirurgiaVazia,totalLinhasInvalidas,totalGeralIdade, totalMeiaIdade, totalTerceiraIdade, rowCount) {
    var relatorio_final = JSON.parse(localStorage.getItem('relatorio.json'))

    relatorio_final[casaEstado].push({
        [nome_arquivo]: {
            totalDtIdadeVazia: totalDtIdadeVazia,
            totalCirurgiaSim: totalCirurgiaSim,
            totalCirurgiaNao: totalCirurgiaNao,
            totalCirurgiaIgnorada: totalCirurgiaIgnorada,
            totalCirurgiaVazia: totalCirurgiaVazia,
            totalLinhasInvalidas: totalLinhasInvalidas,
            totalGeralIdade: totalGeralIdade,
            totalMeiaIdade: totalMeiaIdade,
            totalTerceiraIdade: totalTerceiraIdade,
            rowCount: rowCount
        }
    })

    localStorage.setItem('relatorio.json', JSON.stringify(relatorio_final, null, '\t'))
    
    // if ((estado_atual + 1) != estados.length) {
    //     estado_atual ++
    //     gerar_relatorio()
    // }
}