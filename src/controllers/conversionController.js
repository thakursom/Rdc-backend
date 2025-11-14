const XLSX = require("xlsx");
const { create } = require("xmlbuilder2");
const fs = require("fs");

class conversionController {
    constructor() { }

    async convertXlsxToXml(req, res, next) {
        try {
            if (!req.file) {
                return res.status(400).json({
                    success: false,
                    message: "No file uploaded"
                });
            }

            const workbook = XLSX.readFile(req.file.path);
            const sheetName = workbook.SheetNames[0];
            const sheetData = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName]);

            const xmlObj = {
                root: {
                    row: sheetData.map((item) => ({ item })),
                },
            };

            const xmlContent = create(xmlObj).end({ prettyPrint: true });

            fs.unlinkSync(req.file.path); // delete file

            // Send XML as normal response
            return res.status(200).json({
                success: true,
                xml: xmlContent
            });

        } catch (error) {
            return res.status(500).json({
                success: false,
                message: error.message,
            });
        }
    }

}

module.exports = new conversionController();
