const REGION_LIST = {
    "shenzhen": 'oss-cn-shenzhen'
}

const p = require("path")
const pj = join

const fs = require("fs")
const fse = require("fs-extra")

class BucketClient {
    constructor(accessKeyId, accessKeySecret, bucket, region = REGION_LIST["shenzhen"]) {
        var oss = require('ali-oss');
        this.bucket = bucket
        this.client = oss({
            accessKeyId, accessKeySecret, bucket, region
        });
    }

    static buildUploadtOption(parallel=6, partSize=1024*1024,
                progressFunc = (p, cpt) => { console.log(p, cpt)},
                checkpoint = undefined) {

        const options = {
            parallel,
            partSize,
            progress: function* (p, cpt) {
                progressFunc(p, cpt)
            }
        }
        checkpoint && (options["checkpoint"] = checkpoint)
        return options
    }

    static cloneOptions(options){
        return Object.assign({}, options)
    }

    * uploadFile(inputFilePath, resourceOssKey, options) {
        return yield this.client.multipartUpload(
            resourceOssKey, inputFilePath, options)
    }

    * uploadFileWithRetry(inputFilePath, resourceOssKey, options,
                errorFunc = (error) => console.log("Retry", error)) {
        // TODO: add maximum retry times
        var checkpoint = undefined;
        while (true){
            try{
                options = this.cloneOptions(options)
                const progressFunc = options["progress"]
                options["progress"] = (p, cpt) => {
                    checkpoint = cpt
                    progressFunc(p, cpt)
                }
                return yield this.uploadFile(
                    inputFilePath, resourceOssKey, options)
            } catch(error) {
                errorFunc(error)
            }
        }
    }

    * getKeyListWithPrefix(prefix) {

        var result = yield this.client.list({ prefix })

        var ret = [].concat(result["objects"])
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
        const tempFilePath = Math.random() + ".dy_tmp_junk"
        const result = yield this.client.get(resourceOssKey, tempFilePath);

        fse.copySync(tempFilePath, localFilePath);
        fs.unlinkSync(tempFilePath)
    }

    // TODO: we should consider using recursive
    * uploadFolder(prefix, resourceOssKey, reportFunc, options) {
        const fileList = fs.readdirSync(prefix)
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

            const estimate = i==='0' ? undefined : delta / i * (fileList.length - i)

            const finsihed = accumulateSize[i-1] | 0
            const curSize = accumulateSize[i] - finsihed

            const resource_name = [resourceOssKey, p.basename(file)].join('/')

            options = this.cloneOptions(options)
            options["progress"] = (p, cpt) => {
                try{
                    const finsihedSize = finished + p / 100 * curSize
                    const velocity = finsihedSize / (Date.now() - beginTime)
                    const esitmate = (totalSize - finsihedSize) / velocity 
                    reportFunc && reportFunc(i, fileList.length, file, estimate)
                }catch(error){
                    console.log(error)
                }
            }
            yield this.uploadFileWithRetry(file, resource_name, options)
        }
        return true
    }

}

exports.BucketClient = BucketClient
