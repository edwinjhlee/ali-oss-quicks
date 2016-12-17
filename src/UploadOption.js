class UploadOption{
    constructor(parallel=6, partSize=1024*1024,
                progressFunc = (p, cpt) => { console.log(p, cpt)},
                checkpoint = undefined){
        const options = {
            parallel,
            partSize,
            progress: function* (p, cpt) {
                progressFunc(p, cpt)
            }
        }
        checkpoint && (options["checkpoint"] = checkpoint)
        this.options = options
    }

    clone(){
        new UploadOption(
                this.options["parallel"],
                this.options["partSize"],
                this.options["progress"],
                this.options["checkpoint"])
    }

}
exports.UploadOption = UploadOption