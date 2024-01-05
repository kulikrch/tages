const fs = require('node:fs');
const fsp = require('node:fs/promises');
const readline = require('node:readline');
const v8 = require('node:v8');

// Задание 1тб с 500мб ОЗУ

// Функция разбиения исходного файла на более мелкие сортированные
const createMiniSortFiles = async (inputFilename, outputFileSizeMB) => {
    // Создание директории для хранения мини файлов
    const directoryPath = `miniSort_${new Date().getTime()}`;
    await fsp.mkdir(directoryPath);

    const outputPrefix = `${directoryPath}/sortMiniData`;
    const inputStream = fs.createReadStream(inputFilename, { encoding: 'utf-8' });

    let lines = [];
    let totalSize = 0;
    let fileIndex = 0;

    // Создание интерфейса для чтения из входного файла
    const rl = readline.createInterface({
        input: inputStream
    });

    // Цикл обработки каждой строки из входного файла
    for await (const line of rl) {
        if (totalSize / 1024 / 1024 > outputFileSizeMB) {
            // Опрежедение статистики памяти
            const heapStatistics = v8.getHeapStatistics();

            lines.sort();
            const filename = `${outputPrefix}_${fileIndex}.txt`;
            console.log(fileIndex, heapStatistics.total_heap_size / 1024 / 1024);
            await fsp.writeFile(filename, lines.join('\n'));
            fileIndex++;
            lines = [];
            totalSize = 0;
        }
        lines.push(line);
        totalSize += Buffer.byteLength(line, 'utf-8');
    }

    lines.sort();
    const filename = `${outputPrefix}_${fileIndex}.txt`;
    await fsp.writeFile(filename, lines.join('\n'));

    console.log('Sorting and saving complete');

    return [directoryPath, fileIndex];
}

const updateMapString = ({ done, value}, map_string, fileName) => {
    let flagLine = true;
    while (flagLine) {
        if (!done || value === undefined && done) {
            flagLine = false;
        }
    }

    if (value === '' || value === undefined) {
        map_string.delete(fileName);
        fs.unlink(fileName, (err) => {
            if (err) {
                console.error(`Ошибка при удалении файла: ${err}`);
            } else {
                console.log(`Файл успешно удален: ${fileName}`);
            }
        });
    }
    else {
        map_string.set(fileName, value + '\n');
    }
    return map_string;
};

const processFilesInCircle = async (directoryPath, start, end, addFileIndex) => {
    const outputFileName = `${directoryPath}/sortMiniData_${addFileIndex}.txt`;
    await fsp.writeFile(outputFileName, '');
    // Файл для записи строк
    const file = fs.createWriteStream(outputFileName);
    // Map файлов
    let map_files = new Map();
    let map_string = new Map();

    // Цикл записи в Map всех файлов с записью
    for (let i = start; i < end; i++) {
        const key = `${directoryPath}/sortMiniData_${i}.txt`;
        const rl = readline.createInterface({
            input: fs.createReadStream(key, { encoding: 'utf-8', historySize: 0 })
        });
        map_files.set(key, rl[Symbol.asyncIterator]());
        map_string = updateMapString(await map_files.get(key).next(), map_string, key);
    }
    console.log('Память после открытия интерфейсов в мб', map_string, process.memoryUsage().heapTotal / 1024 / 1024);

    let flag = true;

    while (flag) {
        // Сортировка по строке
        let sortedArray = Array.from(map_string).sort((a, b) => a[1].localeCompare(b[1]));
        // Получение первой Map
        let firstMap = sortedArray[0];

        // Запись строки в цикл
        file.write(firstMap[1]);
        // Запись следующей строки
        map_string = updateMapString(await map_files.get(firstMap[0]).next(), map_string, firstMap[0]);

        flag = map_string.size !== 0;
    }
    file.end();
}

const main = async (inputFilename, outputFileSizeMB, fileCount) => {
    let [dirSortFiles, lastIndex] = await createMiniSortFiles(inputFilename, outputFileSizeMB).catch(error => console.error('Error createMiniSortFiles:', error));
    let i = 0;

    while (i < lastIndex) {
        let step = fileCount;
        if (i + step > ++lastIndex) {
            step = lastIndex - i;
        }

        console.log('processFilesInCircle', lastIndex);

        await processFilesInCircle(dirSortFiles, i, i + step, lastIndex);
        i += step;
    }

    console.log(dirSortFiles);
}

main('random_data.txt', 300, 100)
    .catch(error => console.error('Error processing file:', error));
