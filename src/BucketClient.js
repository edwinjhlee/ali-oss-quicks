const REGION_LIST = {
    "shenzhen": 'oss-cn-shenzhen'
}

const p = require("path")
const pj = p.join

const fs = require("fs")
const fse = require("fs-extra")

class BucketClient {
    constructor(accessKeyId, accessKeySecret, bucket, log = undefined, region = REGION_LIST["shenzhen"]) {
        var oss = require('ali-oss');
        this.bucket = bucket
        this.client = oss({
            accessKeyId, accessKeySecret, bucket, region
        });
    }

    static buildUploadtOption(parallel=6, partSize=1024*600,
                progressFunc,
                checkpoint = undefined) {

        const options = {
            parallel,
            partSize,
            progress: function *(p, cpt) {
                if (progressFunc){
                    return yield progressFunc(p, cpt)
                } else {
                    return true
                }
            }
        }
        checkpoint && (options["checkpoint"] = checkpoint)
        return options
    }

    static cloneOptions(options){
        return Object.assign({}, options)
    }

    static enableLogToFile(dir){
        var access = fs.createWriteStream(dir + '/node.access.log', { flags: 'a' }) 
        var error = fs.createWriteStream(dir + '/node.error.log', { flags: 'a' })

        process.stdout.pipe(access);
        process.stderr.pipe(error);
    }

    * uploadEmptyFile(resourceOssKey){
        return yield this.uploadFile( 
            pj(__dirname, "empty_file"),
            resourceOssKey,
            BucketClient.buildUploadtOption() )
    }

    * uploadFile(inputFilePath, resourceOssKey, options) {
        log && log(inputFilePath, resourceOssKey, options)
        return yield this.client.multipartUpload(
            resourceOssKey, inputFilePath, options)
    }

    * uploadFileWithRetry(inputFilePath, resourceOssKey, options,
                errorFunc = (error) => log && log("Retry", error)) {

        // TODO: add checkpoint
        var checkpoint = undefined;
        var failCount = 0
        while (true){
            try{
                options = BucketClient.cloneOptions(options)
                const progressFunc = options["progress"]
                options["progress"] = function * (p, cpt) {
                    checkpoint = cpt

                    if (progressFunc !== undefined)
                        return yield progressFunc(p, cpt)
                    else return true
                }
                log && log(options)
                return yield this.uploadFile(
                    inputFilePath, resourceOssKey, options)
            } catch(error) {
                errorFunc(error)
                if ((retryTimes >= 0) && (failCount >= retryTimes)){
                    return false
                }
                failCount ++
            }
        }
    }

    * getKeyListWithPrefix(prefix) {

        var result = yield this.client.list({ prefix })

        const result_objects = result["objects"]
        var ret = result_objects === undefined ? [] : result["objects"]
        while (result.isTruncated){
            result = yield this.client.list({
                prefix,
                marker: result.nextMarker
            });
            ret = ret.concat(result["objects"])
        }
        return ret
    }

    // TODO: add retry
    * downloadFile(resourceOssKey, localFilePath) {
        const tempFilePath = Math.random() + ".tmp_junk"
        const result = yield this.client.get(resourceOssKey, tempFilePath);

        fse.copySync(tempFilePath, localFilePath);
        fs.unlinkSync(tempFilePath)
    }

    * catFile(resourceOssKey){
        const tempFilePath = Math.random() + ".tmp_junk"
        const result = yield this.client.get(resourceOssKey, tempFilePath);
        const str = fs.readFileSync(tempFilePath,'utf-8')
        fs.unlinkSync(tempFilePath)
        return str
    }

    * uploadString(content, resourceOssKey){
        const tempFilePath = Math.random() + ".tmp_junk"
        fs.writeFileSync(tempFilePath, content)
        yield this.uploadFileWithRetry(tempFilePath, resourceOssKey, BucketClient.buildUploadtOption())
        fs.unlinkSync(tempFilePath)
        return true
    }

    // TODO: we should consider using recursive
    * uploadFolder(folder, resourceOssKey, reportFunc, options) {
        const fileList = fs.readdirSync(folder)
        for (var i in fileList){
            const f = fileList[i]
            fileList[i] = pj(folder, f)
        }
        return yield this.uploadFiles(fileList, resourceOssKey, reportFunc, options)
    }

    * uploadFiles(fileList, resourceOssKey, reportFunc, options) {

        const accumulateSize = []
        for (var i in fileList){
            accumulateSize[i] = (accumulateSize[i-1] | 0) + fs.statSync(fileList[i])["size"]
        }

        const totalSize = accumulateSize[accumulateSize.length-1]

        var beginTime = Date.now()
        var time = Date.now()
        for (var i in fileList) {
            const file = fileList[i]

            var finished = accumulateSize[i-1] | 0
            var curSize = accumulateSize[i] - finished

            options = BucketClient.cloneOptions(options)

            const progressFunc = options["progress"]
            options["progress"] = function * (p, cpt) {
                progressFunc && (yield *progressFunc(p, cpt))
                try{
                    const elapsedTime = Date.now() - beginTime

                    const finishedSize = finished + p * curSize
                    const velocity = finishedSize / elapsedTime
                    const estimate = (totalSize - finishedSize) / velocity

                    reportFunc && reportFunc(i, fileList.length, file, estimate)
                }catch(error){
                    log && log(error)
                }
            }

            const resource_name = [resourceOssKey, p.basename(file)].join('/')
            yield this.uploadFileWithRetry(file, resource_name, options)
        }
        return true
    }

}

exports.BucketClient = BucketClient
