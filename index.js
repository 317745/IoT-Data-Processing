require('dotenv').config();

const { dateRange } = require('danfojs');
const { MongoClient } = require('mongodb');
const uri = process.env.CONN;

const client = new MongoClient(uri);
let conn;

let getDb = async () => {
    if (!conn) {
        try {
            conn = await client.connect()
            console.log('Cliente conectado')
        } catch (error) {
            console.log(error)
        }
    }
    return conn.db('LumiCert');
}

let validationTrue = (response) => {
    let boolResponse = response['ok']
    if (!boolResponse) {
        let responseData = response['data']
        throw new Error(responseData)
    }
}

const getDataMedicion = async () => {
    let result;
    try {
        const db = await getDb();
        const dataMedicion = await db
            .collection('medicion1')
            .find()
            .toArray();

        result = {
            'ok': true,
            'msg': 'Los datos fueron obtenidos correctamente.',
            'data': dataMedicion
        }

        return result;
    }
    catch (error) {
        result = {
            'ok': false,
            'msg': 'Error al obtener los datos de medición.',
            'data': error
        }

        return result;
    } finally {
        console.log(result['msg'])
    }
};

const postDataMedicion = async () => {
    let result;
    try {
        let dataInsert = await getDataMedicion();
        validationTrue(dataInsert);

        dataInsert = dataInsert['data'].map(
            (x) => {
                x['fecha'] = new Date(x['fecha']);
                return x;
            }
        );

        const db = await getDb();

        let insertion = await db
            .collection('medicion1')
            .insertMany(
                dataInsert
            )

        if (!insertion.acknowledged) {
            result = {
                'ok': false,
                'msg': 'Hubo un error al insertar los documentos.',
                'data': null
            }

            return result;
        }

        result = {
            'ok': true,
            'msg': 'La inserción de los documentos fue correcta.',
            'data': null
        }

        return result;
    } catch (error) {
        result = {
            'ok': false,
            'msg': 'Error al realizar la inserción de los documentos.',
            'data': error
        }

        return result;
    } finally {
        console.log(result['msg'])
    }
};

const patchYearMedicion = async () => {
    let _id;
    try {
        let dataMediciones = await getDataMedicion();
        validationTrue(dataMediciones);

        dataMediciones = dataMediciones['data'];
        let wrongDates = dataMediciones.filter(
            (x) => new Date(x['fecha']) < new Date('2025-01-01')
        )

        if (wrongDates.length == 0) {
            result = {
                'ok': true,
                'msg': 'No se encuentran fechas con datos incoherentes.',
                'data': null
            }
            return result;
        }

        let goodDates = wrongDates.map(
            (x) => {
                let date = new Date(x['fecha'])
                let day = date.getDate();
                let month = date.getMonth() + 1;
                x['fecha'] = new Date(`2025-${month}-${day}`)

                return x
            }
        );

        let ids = wrongDates.map(
            (x) => x['_id']
        )

        let db = await getDb();

        const deleteWrongDates = await db
        .collection('medicion1')
        .deleteMany(
            {_id : { $in: ids }}
        )   

        if (!deleteWrongDates.acknowledged) {
            let result = {
                'ok': false,
                'msg': 'Ocurrio un error al arreglar las fechas. (ELIMINACION)',
                'data': null
            }
            return result;
        }

        const insertDates = await db
        .collection('medicion1')
        .insertMany(
            goodDates
        )

        if (!insertDates.acknowledged) {
            let result = {
                'ok': false,
                'msg': 'Ocurrio un error al arreglar las fechas. (INSERCION)',
                'data': null
            }
            return result;
        }

        result = {
            'ok': true,
            'msg': 'La validacion y correccion de fechas fue correcta.',
            'data': null
        }

        return result;
    } catch (error) {
        let result = {
                'ok': false,
                'msg': error,
                'data': null
            }
        return result;
    } finally {
        console.log(result['msg']);
    }
};

const deleteDataMedicion = async () => {
    let result;
    try {
        const db = await getDb();

        let delteData = await db
            .collection('medicion1')
            .deleteMany(
                {
                    $or:[{ processed: { $exists: false } }, { processed: false }] 
                }
            )

        if (!delteData.acknowledged) {
            result = {
                'ok': false,
                'msg': 'Hubo un error al eliminar los documentos.',
                'data': null
            }

            return result;
        }

        result = {
            'ok': true,
            'msg': 'Los documentos fueron eliminados correctamente.',
            'data': null
        }

        return result;
    } catch (error) {
        result = {
            'ok': false,
            'msg': 'Error al eliminar los documentos.',
            'data': error
        }

        return result;
    } finally {
        console.log(result['msg'])
    }
}

const patchMedicion = async () => {
    let result;
    try {
        let dataProcess = await getDataMedicion();
        validationTrue(dataProcess);

        dataProcess = dataProcess['data'];

        let dataMediciones = dataProcess.filter(
            (x) => !x.processed
        )

        if (dataMediciones.length == 0) {
            result = {
                'ok': true, 
                'msg': 'No hay nuevos datos.',
                'data': dataProcess
            }
            return result;
        }

        const processedMedicion = dataMediciones.map(
            (x) => {
                    x.processed = true;
                    return x;
            }
        )

        let deletOldMediciones = await deleteDataMedicion();
        validationTrue(deletOldMediciones);

        const db = await getDb();

        let modificationMediciones = await db
            .collection("medicion1")
            .insertMany(
                processedMedicion
            )

        if (!modificationMediciones.acknowledged) {
            result = {
                'ok': false,
                'msg': 'Hubo un error al insertar los documentos procesados.',
                'data': null
            }

            return result;
        }

        result = {
            'ok': true,
            'msg': 'Los documentos procesados fueron insertados correctamente.',
            'data': processedMedicion
        }

        return result;
    } catch (error) {
        result = {
            'ok': false,
            'msg': 'Error al modificar las mediciones.',
            'data': error
        }

        return result;
    } finally {
        console.log(result['msg'])
    }
}

const dataProcessing = async () => {
    let result;
    try {
        let dataToProcess = await patchMedicion();
        validationTrue(dataToProcess);

        dataToProcess = dataToProcess['data'];

        let dataGroupedId = {};
        let lum;

        for (let luminarias = 0; luminarias < dataToProcess.length; luminarias++) {
            lum = dataToProcess[luminarias];
            let id_lum = `lum_${lum['id_luminaria']}`

            if (!dataGroupedId[id_lum]) {
                dataGroupedId[id_lum] = []
                dataGroupedId[id_lum].push(lum);

            } else {
                dataGroupedId[id_lum].push(lum);
            }
        }

        let keyLum = Object.keys(dataGroupedId);
        let lumYear = {};

        keyLum.forEach(lum => {
            let lumGroup = dataGroupedId[lum];
            let yearsGrouped = {};

            for (let i = 0; i < lumGroup.length; i++) {
                let date = new Date(lumGroup[i]['fecha'])
                let year = date.getFullYear();
                let year_id = `year_${year}`

                if (!yearsGrouped[year_id]) {
                    yearsGrouped[year_id] = [];
                    yearsGrouped[year_id].push(lumGroup[i])

                } else {
                    yearsGrouped[year_id].push(lumGroup[i])
                }
            }

            lumYear[lum] = yearsGrouped;
        })

        let groupedLums = {}

        keyLum.forEach(lum => {
            let lumById = lumYear[lum]
            let keyYears = Object.keys(lumById)
            let years = {}

            keyYears.forEach(year => {
                let yearGrouped = lumById[year]
                let months = {};

                for (let m = 0; m < yearGrouped.length; m++) {
                    let date = new Date(yearGrouped[m]['fecha']);
                    let month = date.getMonth();
                    let idMonth = `month_${month}`

                    if (!months[idMonth]) {
                        months[idMonth] = []
                        months[idMonth].push(yearGrouped[m])
                    } else {
                        months[idMonth].push(yearGrouped[m])
                    }
                }

                years[year] = months;
            })

            groupedLums[lum] = years;
        })

        result = {
            'ok': true,
            'msg': 'La agrupacion de datos por luminaria fue correcta.',
            'data': groupedLums
        };

        return result;

    } catch (error){
        result = {
            'ok': false,
            'msg': 'Error al modificar las mediciones.',
            'data': error
        }

        return result;
    } finally {
        console.log(result['msg'])
    }
}

const consumoMonth = async () => {
    try {
        let groupedConsum = await dataProcessing();
        validationTrue(groupedConsum);

        groupedConsum = groupedConsum['data']

        let consumoLuminarias = [];
        lums_id = Object.keys(groupedConsum);

        lums_id.forEach(lumKey => {
            let luminaria = groupedConsum[lumKey];
            let years_id = Object.keys(luminaria);

            years_id.forEach(yearKey => {
                let year = luminaria[yearKey]
                let months_id = Object.keys(year)

                months_id.forEach(monthKey =>{
                    let month = year[monthKey]

                    let consumo = month.reduce(
                        (acc, obj) => acc + obj['consumo'], 
                        0
                    )

                    let consumoLum = {}

                    consumoLum['id_lum'] = lumKey;
                    consumoLum['id_year'] = yearKey;
                    consumoLum['id_month'] = monthKey;
                    consumoLum['consumoMensual'] = consumo;

                    consumoLuminarias.push(consumoLum);
                })
            })
        })

        result = {
            'ok': true,
            'msg': 'El calculo de consumo mensual se realizo de manera correcta',
            'data': consumoLuminarias
        };

        return result;

    } catch (error) {
        result = {
            'ok': false,
            'msg': 'Error al calcular el consumo mensual.',
            'data': error
        }

        return result;  
    } finally {
        console.log(result)
        await client.close();
    }
}

//getDataMedicion();
//postDataMedicion();
//patchYearMedicion();
//patchMedicion();
//dataProcessing();
consumoMonth();