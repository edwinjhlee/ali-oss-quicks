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

        this.log = log
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

    * uploadEmptyFile(resourceOssKey, errorFunc = undefined, retryTimes = -1){
        return yield this.uploadFile( 
            pj(__dirname, "empty_file"),
            resourceOssKey,
            BucketClient.buildUploadtOption(),
            errorFunc, retryTimes)
    }

    * uploadFile(inputFilePath, resourceOssKey, options) {
        this.log && this.log(inputFilePath, resourceOssKey, options)
        if (options === undefined) {
            options = BucketClient.buildUploadtOption()
        }
        return yield this.client.multipartUpload(
            resourceOssKey, inputFilePath, options)
    }

    * uploadFileWithRetry(inputFilePath, resourceOssKey, options, errorFunc, retryTimes) {

        if (options === undefined) {
            options = BucketClient.buildUploadtOption()
        }

        if (errorFunc === undefined){
            errorFunc = (error) => this.log && this.log("Retry", error)
        }

        if (retryTimes === undefined){
            retryTimes = -1
        }

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
                if (checkpoint !== undefined){
                    options["checkpoint"] = checkpoint
                }
                this.log && this.log(options)
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

    * uploadString(content, resourceOssKey, options, errorFunc, retryTimes){
        const tempFilePath = Math.random() + ".tmp_junk"
        fs.writeFileSync(tempFilePath, content)
        yield this.uploadFileWithRetry(tempFilePath, resourceOssKey, options, errorFunc, retryTimes)
        fs.unlinkSync(tempFilePath)
        return true
    }

    // TODO: we should consider using recursive
    * uploadFolder(folder, resourceOssKey, reportFunc, options, errorFunc, retryTimes) {
        const fileList = fs.readdirSync(folder)
        for (var i in fileList){
            const f = fileList[i]
            fileList[i] = pj(folder, f)
        }
        return yield this.uploadFiles(fileList, resourceOssKey, reportFunc, options, errorFunc, retryTimes)
    }

    * uploadFiles(fileList, resourceOssKey, reportFunc, options, errorFunc, retryTimes) {

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
                    this.log && this.slog(error)
                }
            }

            const resource_name = [resourceOssKey, p.basename(file)].join('/')
            yield this.uploadFileWithRetry(file, resource_name, options, errorFunc, retryTimes)
        }
        return true
    }

}

exports.BucketClient = BucketClient
