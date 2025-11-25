const path = require("path");
const fs = require("fs");

const XLSX = require("xlsx");
const RevenueUpload = require("../models/RevenueUploadModel");
const AppleRevenue = require("../models/AppleRevenueModel");
const SpotifyRevenue = require("../models/SpotifyRevenueModel");
const GaanaRevenue = require("../models/GaanaRevenueModel");
const JioSaavanRevenue = require("../models/JioSaavanRevenueModel");
const FacebookRevenue = require("../models/FacebookRevenueModel");
const AmazonRevenue = require("../models/AmazonRevenueModel");
const TikTokRevenue = require("../models/TikTokRevenueModel");



class revenueUploadController {

    constructor() { }

    //uploadRevenue method
    async uploadRevenue(req, res, next) {
        try {
            const { userId } = req.user;
            const { platform, periodFrom, periodTo } = req.body;

            if (!req.file) {
                return res.status(400).json({ error: "No file uploaded" });
            }

            // Only relative path
            const relativePath = `uploads/revenues/${req.file.filename}`;

            // Use BASE_URL from env ALWAYS
            const fileURL = `${process.env.BASE_URL}/${relativePath}`;

            const RevenueUploads = await RevenueUpload.create({
                user_id: userId,
                platform,
                periodFrom,
                periodTo,
                fileName: req.file.filename,
                filePath: fileURL,
                fileExt: req.file.mimetype,
            });

            // Excel Parsing
            const workbook = XLSX.readFile(req.file.path);
            const sheet = workbook.Sheets[workbook.SheetNames[0]];
            const jsonData = XLSX.utils.sheet_to_json(sheet);

            if (jsonData.length === 0) {
                return res.status(400).json({ error: "Excel file is empty" });
            }

            const rows = jsonData.map(row => ({
                uploadId: RevenueUploads._id,
                ...row
            }));

            const modelMap = {
                Spotify: SpotifyRevenue,
                AppleItunes: AppleRevenue,
                Gaana: GaanaRevenue,
                JioSaavan: JioSaavanRevenue,
                Facebook: FacebookRevenue,
                Amazon: AmazonRevenue,
                TikTok: TikTokRevenue
            };

            if (modelMap[platform]) {
                await modelMap[platform].insertMany(rows);
            }

            return res.json({
                success: true,
                message: "File uploaded and processed successfully",
                uploadId: RevenueUploads._id,
                fileURL: fileURL
            });

        } catch (error) {
            console.log(error);
            res.status(500).json({ error: error.message });
        }
    }



    //getAllRevenueUploads method
    async getAllRevenueUploads(req, res, next) {
        try {
            let { page = 1, limit = 20, platform } = req.query;

            page = parseInt(page);
            limit = parseInt(limit);

            const query = {};

            // Optional filter by platform
            if (platform) {
                query.platform = platform;
            }

            const skip = (page - 1) * limit;

            const [data, total] = await Promise.all([
                RevenueUpload.find(query)
                    .sort({ createdAt: -1 })
                    .skip(skip)
                    .limit(limit),

                RevenueUpload.countDocuments(query)
            ]);

            return res.json({
                success: true,
                message: "Revenue uploads fetched successfully",
                currentPage: page,
                totalPages: Math.ceil(total / limit),
                totalRecords: total,
                data
            });

        } catch (error) {
            console.log(error);
            return res.status(500).json({ success: false, error: error.message });
        }
    }


}

module.exports = new revenueUploadController();