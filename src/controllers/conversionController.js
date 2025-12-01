const XLSX = require("xlsx");
const { create } = require("xmlbuilder2");
const fs = require("fs");
const LogService = require("../services/logService");

class conversionController {
    constructor() { }

    //convertXlsxToXml method
    async convertXlsxToXml(req, res, next) {
        try {
            const { userId, email } = req.user;
            if (!req.file) {
                return res.status(400).json({
                    success: false,
                    message: "No file uploaded"
                });
            }

            const workbook = XLSX.readFile(req.file.path);
            const sheetName = workbook.SheetNames[0];
            const sheetData = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName]);

            // Function to sanitize XML keys
            const sanitizeKey = (key) => {
                return key
                    .replace(/[^a-zA-Z0-9_]/g, "_")
                    .replace(/^[0-9]/, "_$&");
            };

            // Convert rows with sanitized keys
            const cleanedRows = sheetData.map(row => {
                const cleaned = {};
                Object.keys(row).forEach(key => {
                    cleaned[sanitizeKey(key)] = row[key];
                });
                return cleaned;
            });

            const xmlObj = {
                root: {
                    row: cleanedRows
                }
            };

            const xmlContent = create(xmlObj).end({ prettyPrint: true });

            fs.unlinkSync(req.file.path); // delete original file

            await LogService.createLog({
                user_id: userId,
                email,
                action: "CONVERT_XLSX_TO_XML_SUCCESS",
                description: "XLSX file converted to XML successfully",
                newData: {
                    fileName: req.file.filename,
                    totalRows: cleanedRows.length,
                    xmlPreview: xmlContent.substring(0, 300) + "..." // Avoid overloading DB
                },
                req
            });

            return res.status(200).json({
                success: true,
                xml: xmlContent
            });

        } catch (error) {
            next(error);
        }
    }

}

module.exports = new conversionController();
