const XLSX = require("xlsx");

const RevenueUpload = require("../models/RevenueUploadModel");
const AppleRevenue = require("../models/AppleRevenueModel");
const SpotifyRevenue = require("../models/SpotifyRevenueModel");
const GaanaRevenue = require("../models/GaanaRevenueModel");
const JioSaavanRevenue = require("../models/JioSaavanRevenueModel");
const FacebookRevenue = require("../models/FacebookRevenueModel");
const AmazonRevenue = require("../models/AmazonRevenueModel");
const TikTokRevenue = require("../models/TikTokRevenueModel");
const TempReport = require("../models/tempReportModel");
const TblReport2025 = require("../models/tblReport2025Model");
const LogService = require("../services/logService");
const ExcelJS = require('exceljs');




class revenueUploadController {

    constructor() {
        this.downloadJobs = new Map();
        this.JOB_EXPIRY_TIME = 24 * 60 * 60 * 1000; // 24 hours

        // Cleanup expired jobs every hour
        setInterval(() => this.cleanupExpiredJobs(), 3600000);
    }


    //uploadRevenue method
    async uploadRevenue(req, res, next) {
        try {
            const { userId, email } = req.user;
            const { platform, periodFrom, periodTo } = req.body;

            if (!req.file) {
                return res.status(400).json({ error: "No file uploaded" });
            }

            const relativePath = `uploads/revenues/${req.file.filename}`;
            const fileURL = `${process.env.BASE_URL}/${relativePath}`;

            const RevenueUploads = await RevenueUpload.create({
                user_id: 0,
                platform,
                periodFrom,
                periodTo,
                fileName: req.file.filename,
                filePath: fileURL,
                fileExt: req.file.mimetype,
            });

            // Read Excel
            const workbook = XLSX.readFile(req.file.path);
            const sheet = workbook.Sheets[workbook.SheetNames[0]];
            const jsonData = XLSX.utils.sheet_to_json(sheet);

            if (jsonData.length === 0) {
                return res.status(400).json({ error: "Excel file is empty" });
            }

            // const rows = jsonData.map(row => ({
            //     uploadId: RevenueUploads._id,
            //     ...row
            // }));

            const mappedRows = jsonData.map(r => {
                let obj = {};

                // FACEBOOK MAPPING
                if (platform === "Facebook") {
                    obj = {
                        retailer: r.service || null,
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: r.elected_isrc || null,
                        release: r.track_title || null,
                        track_title: r.track_title || null,
                        track_artist: r.track_artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r.event_count_1 || null,
                        sale_type: null,
                        net_total: r["Total Revenue"] || null,
                    };

                    // SPOTIFY MAPPING
                } else if (platform === "Spotify") {
                    obj = {
                        retailer: "Spotify",
                        label: r["Label Name"] || null,
                        upc_code: r.EAN || null,
                        catalogue_number: null,
                        isrc_code: r.ISRC || null,
                        release: r["Album name"] || null,
                        track_title: r["Track name"] || null,
                        track_artist: r["Artist name"] || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r.Quantity || null,
                        sale_type: null,
                        net_total: r.Total || null,
                    };

                    // Amazon MAPPING
                } else if (platform === "Amazon") {
                    obj = {
                        retailer: "Amazon",
                        label: r["Label Name"] || null,
                        upc_code: r["Digital Album Upc"] || null,
                        catalogue_number: null,
                        isrc_code: r.ISRC || null,
                        release: r["Album Name"] || null,
                        track_title: r["Track Name"] || null,
                        track_artist: r["Artist Name"] || null,
                        remixer_name: null,
                        remix: null,
                        territory: r["territory code"] || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r["Total Plays"] || null,
                        sale_type: null,
                        net_total: r[" Total Revenue"] || null,
                    };

                    //JioSaavan MAPPING
                } else if (platform === "JioSaavan") {
                    obj = {
                        retailer: "jio_savan",
                        label: r["Label Name"] || null,
                        upc_code: r.UPC || null,
                        catalogue_number: null,
                        isrc_code: r.ISRC || null,
                        release: r["Album Name"] || null,
                        track_title: r["Track Name"] || null,
                        track_artist: r["Artist Name"] || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r["Total Streams"] || null,
                        sale_type: null,
                        net_total: r["Total Revenue"] || null,
                    };

                    //AppleItunes MAPPING
                } else if (platform === "AppleItunes") {
                    obj = {
                        retailer: "Apple Music",
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: r.ISRC || null,
                        release: r["Item Title"] || null,
                        track_title: r["Item Title"] || null,
                        track_artist: r["Item Artist"] || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r.Quantity || null,
                        sale_type: null,
                        net_total: r["Total Revenue"] || null,
                    };

                    //TikTok MAPPING
                } else if (platform === "TikTok") {
                    obj = {
                        retailer: r.platforn_name,
                        label: r["Label Name"] || null,
                        upc_code: r.product_code,
                        catalogue_number: null,
                        isrc_code: r.isrc || null,
                        release: r.album || null,
                        track_title: r.song_title || null,
                        track_artist: r.artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: r.content_type || null,
                        track_count: r.video_views || null,
                        sale_type: null,
                        net_total: r["INR Revenue"] || null,
                    };

                    //Gaana MAPPING
                } else if (platform === "Gaana") {
                    obj = {
                        retailer: "Gaana",
                        label: r["Label Name"] || null,
                        upc_code: r["Album UPC"],
                        catalogue_number: null,
                        isrc_code: r.ISRC || null,
                        release: r.Album || null,
                        track_title: r["Track Title"] || null,
                        track_artist: r.Artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: r.content_type || null,
                        track_count: r["Total Playouts"] || null,
                        sale_type: null,
                        net_total: r["Total"] || null,
                    };
                }

                const today = new Date().toISOString().split("T")[0];
                obj.date = today;
                obj.user_id = 0;
                obj.uploading_date = today;
                obj.uploadId = RevenueUploads._id;

                return obj;
            });


            // Insert platform-specific revenue table
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
                await modelMap[platform].insertMany(mappedRows);
            }

            // Now insert the new mapped rows
            await TempReport.insertMany(mappedRows);

            await LogService.createLog({
                user_id: userId,
                email,
                action: `ADD_REVENUE_FOR_${platform}`,
                description: `${platform} revenue uploaded successfully`,
                newData: mappedRows,
                req
            });

            return res.json({
                success: true,
                message: "File uploaded and processed successfully",
                uploadId: RevenueUploads._id,
                fileURL: fileURL
            });

        } catch (error) {
            next(error);
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
            next(error)
        }
    }

    //getRevenueById method
    async getRevenueById(req, res, next) {
        try {
            const { userId, page = 1, limit = 20 } = req.query;

            if (!userId) {
                return res.status(400).json({
                    success: false,
                    message: "User ID is required"
                });
            }

            // const numericUserId = parseInt(userId);
            const pageNum = parseInt(page);
            const limitNum = parseInt(limit);
            const skip = (pageNum - 1) * limitNum;

            const revenues = await TempReport.find({
                uploadId: userId
            })
                .sort({ uploading_date: -1 })
                .skip(skip)
                .limit(limitNum);

            const totalCount = await TempReport.countDocuments({
                uploadId: userId
            });

            const totalPages = Math.ceil(totalCount / limitNum);

            return res.status(200).json({
                success: true,
                message: "Revenue data retrieved successfully",
                data: revenues,
                pagination: {
                    currentPage: pageNum,
                    totalPages: totalPages,
                    totalCount: totalCount,
                    hasNext: pageNum < totalPages,
                    hasPrev: pageNum > 1
                }
            });

        } catch (error) {
            console.error("Error fetching revenue by user ID:", error);
            return res.status(500).json({
                success: false,
                message: "Internal server error"
            });
        }
    }

    //uploadTblRevenue method
    async uploadTblRevenue(req, res, next) {
        try {
            const { userId, email } = req.user;
            const { uploadId } = req.query;

            if (!uploadId) {
                return res.status(400).json({ success: false, message: "uploadId required" });
            }

            // Find and update the revenue upload
            const revenueUpload = await RevenueUpload.findByIdAndUpdate(
                uploadId,
                { isAccepted: true },
                { new: true }
            );

            if (!revenueUpload) {
                return res.status(404).json({
                    success: false,
                    message: "Revenue upload not found"
                });
            }

            //Get all TempReport rows for this uploadId
            const tempData = await TempReport.find({ uploadId }).lean();

            if (!tempData.length) {
                return res.status(404).json({ success: false, message: "No data found for this uploadId" });
            }

            // Remove MongoDB _id field so it can be inserted fresh
            const cleanedData = tempData.map(row => {
                const { _id, ...rest } = row;
                return rest;
            });

            //Insert into TblReport2025 in bulk
            await TblReport2025.insertMany(cleanedData);

            // Delete all existing data from TempReport
            await TempReport.deleteMany({ uploadId });

            await LogService.createLog({
                user_id: userId,
                email,
                action: `REVENUE_ADDED_IN_TBLREPORT_FOR_${tempData.retailer}`,
                description: `${tempData.retailer} revenue uploaded successfully in tbl_report`,
                newData: mappedRows,
                req
            });

            return res.status(200).json({
                success: true,
                message: "Data moved from TempReport to TblReport_2025 successfully",
                insertedCount: cleanedData.length
            });

        } catch (error) {
            console.error("uploadData error:", error);
            return res.status(500).json({
                success: false,
                message: "Something went wrong",
                error: error.message
            });
        }
    }

    // getRevenueReport method
    async getRevenueReport(req, res, next) {
        try {
            const {
                platform,
                month,
                quarter,
                releases,
                artist,
                track,
                partner,
                contentType,
                format,
                territory,
                quarters,
                page = 1,
                limit = 10,
                sortBy = 'date',
                sortOrder = 'desc'
            } = req.query;

            const userId = req.user.id;

            // Build filter object - start with user filter
            const filter = { user: userId }; // Assuming reports belong to specific user

            // Platform filter
            if (platform && platform !== '') {
                filter.retailer = platform;
            }

            // Month filter - handle both string and number
            if (month && month !== '') {
                const year = new Date().getFullYear();
                const monthStr = String(month).padStart(2, '0');
                const startDate = new Date(year, parseInt(month) - 1, 1);
                const endDate = new Date(year, parseInt(month), 0);

                filter.date = {
                    $gte: startDate.toISOString().split('T')[0], // Format as YYYY-MM-DD
                    $lte: endDate.toISOString().split('T')[0]
                };
            }

            // Quarter filter
            if (quarter && quarter !== '') {
                const quarterMonths = {
                    '1': [1, 2, 3],
                    '2': [4, 5, 6],
                    '3': [7, 8, 9],
                    '4': [10, 11, 12]
                };

                if (quarterMonths[quarter]) {
                    const year = new Date().getFullYear();
                    const months = quarterMonths[quarter];
                    const start = new Date(year, months[0] - 1, 1);
                    const end = new Date(year, months[2], 0); // Last day of last month in quarter

                    filter.date = {
                        $gte: start.toISOString().split('T')[0],
                        $lte: end.toISOString().split('T')[0]
                    };
                }
            }

            // Checkbox filters
            if (artist === 'true') {
                filter.track_artist = { $exists: true, $ne: '' };
            }

            if (territory === 'true') {
                filter.territory = { $exists: true, $ne: '' };
            }

            // Add other checkbox filters as needed
            if (releases === 'true') {
                filter.release = { $exists: true, $ne: '' };
            }

            console.log("Filter Object:", JSON.stringify(filter, null, 2));

            // Main aggregation pipeline
            const pipeline = [
                { $match: filter },
                {
                    $group: {
                        _id: {
                            date: "$date",
                            retailer: "$retailer",
                            artist: "$track_artist",
                            release: "$release"
                        },
                        totalStreams: {
                            $sum: {
                                $cond: [
                                    { $eq: [{ $type: "$track_count" }, "string"] },
                                    { $toInt: "$track_count" },
                                    { $ifNull: ["$track_count", 0] }
                                ]
                            }
                        },
                        totalRevenue: {
                            $sum: {
                                $cond: [
                                    { $eq: [{ $type: "$net_total" }, "string"] },
                                    { $toDouble: "$net_total" },
                                    { $ifNull: ["$net_total", 0] }
                                ]
                            }
                        }
                    }
                },
                {
                    $sort: {
                        "_id.date": sortOrder === 'desc' ? -1 : 1,
                        "_id.retailer": 1,
                        "totalRevenue": -1
                    }
                }
            ];

            // Get total count before pagination
            const countPipeline = [...pipeline];
            countPipeline.push({ $count: "totalRecords" });

            // Add pagination to main pipeline
            const skip = (parseInt(page) - 1) * parseInt(limit);
            pipeline.push({ $skip: skip });
            pipeline.push({ $limit: parseInt(limit) });

            // Get summary statistics
            const summaryPipeline = [
                { $match: filter },
                {
                    $group: {
                        _id: null,
                        totalStreams: {
                            $sum: {
                                $cond: [
                                    { $eq: [{ $type: "$track_count" }, "string"] },
                                    { $toInt: "$track_count" },
                                    { $ifNull: ["$track_count", 0] }
                                ]
                            }
                        },
                        totalRevenue: {
                            $sum: {
                                $cond: [
                                    { $eq: [{ $type: "$net_total" }, "string"] },
                                    { $toDouble: "$net_total" },
                                    { $ifNull: ["$net_total", 0] }
                                ]
                            }
                        }
                    }
                }
            ];

            const [data, countResult, summary] = await Promise.all([
                TblReport2025.aggregate(pipeline),
                TblReport2025.aggregate(countPipeline),
                TblReport2025.aggregate(summaryPipeline)
            ]);

            const totalRecords = countResult[0]?.totalRecords || 0;
            const totalPages = Math.ceil(totalRecords / parseInt(limit));

            const summaryData = summary[0] || {
                totalStreams: 0,
                totalRevenue: 0
            };

            res.json({
                success: true,
                data: {
                    summary: {
                        totalStreams: summaryData.totalStreams,
                        totalRevenue: parseFloat(summaryData.totalRevenue.toFixed(2)),
                        platforms: [], // You can add this if needed
                        artists: [],   // You can add this if needed
                        releases: []   // You can add this if needed
                    },
                    reports: data.map(item => ({
                        date: item._id.date,
                        platform: item._id.retailer || 'Unknown',
                        artist: item._id.artist || 'Unknown',
                        release: item._id.release || 'Unknown',
                        streams: item.totalStreams,
                        revenue: parseFloat(item.totalRevenue.toFixed(2))
                    })),
                    pagination: {
                        totalRecords,
                        totalPages,
                        currentPage: parseInt(page),
                        limit: parseInt(limit)
                    }
                }
            });

        } catch (error) {
            console.error("Error in getRevenueReport:", error);
            next(error);
        }
    }

    // downloadExcelReport method
    async downloadExcelReport(req, res, next) {
        try {
            console.log('=== Starting Excel Export ===');

            const {
                platform,
                month,
                quarter,
                releases,
                artist,
                track,
                partner,
                contentType,
                format,
                territory,
                quarters
            } = req.query;

            const userId = req.user.id;

            // Build filter object
            const filter = { user: userId };

            // Platform filter
            if (platform && platform !== '') {
                filter.retailer = platform;
            }

            // Month filter
            if (month && month !== '') {
                const year = new Date().getFullYear();
                const startDate = new Date(year, parseInt(month) - 1, 1);
                const endDate = new Date(year, parseInt(month), 0);

                filter.date = {
                    $gte: startDate.toISOString().split('T')[0],
                    $lte: endDate.toISOString().split('T')[0]
                };
            }

            // Quarter filter
            if (quarter && quarter !== '') {
                const quarterMonths = {
                    '1': [1, 2, 3],
                    '2': [4, 5, 6],
                    '3': [7, 8, 9],
                    '4': [10, 11, 12]
                };

                if (quarterMonths[quarter]) {
                    const year = new Date().getFullYear();
                    const months = quarterMonths[quarter];
                    const start = new Date(year, months[0] - 1, 1);
                    const end = new Date(year, months[2], 0);

                    filter.date = {
                        $gte: start.toISOString().split('T')[0],
                        $lte: end.toISOString().split('T')[0]
                    };
                }
            }

            // Checkbox filters
            if (artist === 'true') {
                filter.track_artist = { $exists: true, $ne: '' };
            }

            if (territory === 'true') {
                filter.territory = { $exists: true, $ne: '' };
            }

            if (releases === 'true') {
                filter.release = { $exists: true, $ne: '' };
            }

            console.log('Export filter:', JSON.stringify(filter, null, 2));

            // Get data WITHOUT aggregation for simplicity
            const data = await TblReport2025.find(filter)
                .select('date retailer track_artist release track_count net_total')
                .sort({ date: -1 })
                .lean();

            console.log(`Found ${data.length} records for export`);

            if (data.length === 0) {
                // Send a proper error response
                return res.status(404).json({
                    success: false,
                    message: "No data found to export"
                });
            }

            // **FIX: Format data CORRECTLY**
            const excelData = [];

            // Add headers
            const headers = ['S.No', 'Date', 'Platform', 'Artist', 'Release', 'Streams', 'Revenue'];
            excelData.push(headers);

            // Add data rows
            data.forEach((item, index) => {
                // Format date properly
                let formattedDate = 'N/A';
                if (item.date) {
                    try {
                        const date = new Date(item.date);
                        if (!isNaN(date.getTime())) {
                            formattedDate = date.toLocaleDateString('en-GB');
                        }
                    } catch (e) {
                        formattedDate = item.date;
                    }
                }

                // Convert numbers
                const streams = Number(item.track_count) || 0;
                const revenue = parseFloat(item.net_total || 0);

                excelData.push([
                    index + 1,
                    formattedDate,
                    item.retailer || 'N/A',
                    item.track_artist || 'N/A',
                    item.release || 'N/A',
                    streams,
                    revenue.toFixed(2) // Keep 2 decimal places for currency
                ]);
            });

            // **FIX: Create workbook properly**
            const workbook = XLSX.utils.book_new();

            // Use `aoa_to_sheet` (Array of Arrays) instead of `json_to_sheet`
            const worksheet = XLSX.utils.aoa_to_sheet(excelData);

            // Set column widths
            worksheet['!cols'] = [
                { wch: 8 },   // S.No
                { wch: 12 },  // Date
                { wch: 20 },  // Platform
                { wch: 25 },  // Artist
                { wch: 30 },  // Release
                { wch: 12 },  // Streams
                { wch: 15 }   // Revenue
            ];

            // Add worksheet to workbook
            XLSX.utils.book_append_sheet(workbook, worksheet, "Revenue Report");

            // Generate filename - SIMPLIFY it
            const timestamp = new Date().toISOString().split('T')[0].replace(/-/g, '');
            const filename = `Revenue_Report_${timestamp}.xlsx`;

            console.log(`Creating Excel file: ${filename}`);

            // **FIX: Write to buffer with proper options**
            const excelBuffer = XLSX.write(workbook, {
                type: 'buffer',
                bookType: 'xlsx',
                bookSST: false
            });

            console.log(`Excel buffer size: ${excelBuffer.length} bytes`);

            // **FIX: Set headers CORRECTLY**
            res.writeHead(200, {
                'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'Content-Disposition': `attachment; filename="${filename}"`,
                'Content-Length': excelBuffer.length,
                'Cache-Control': 'no-cache, no-store, must-revalidate',
                'Pragma': 'no-cache',
                'Expires': '0'
            });

            // **FIX: Send the buffer**
            res.end(excelBuffer);
            console.log('=== Excel file sent successfully ===');

        } catch (error) {
            console.error("Error in downloadExcelReport:", error);

            // Send JSON error if headers not sent yet
            if (!res.headersSent) {
                res.status(500).json({
                    success: false,
                    message: "Error generating Excel file",
                    error: error.message
                });
            }
        }
    }


}

module.exports = new revenueUploadController();