const fs = require('fs');
const path = require('path');
const XLSX = require("xlsx");
const { chain } = require('stream-chain');
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/StreamArray');
const mongoose = require("mongoose");
const Papa = require('papaparse');
const fastCsv = require('fast-csv');

const LogService = require("../services/logService");
const User = require("../models/userModel");
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
const SoundRecordingRevenue = require("../models/soundRecordingRevenueModel");
const YouTubeArtTrackRevenue = require("../models/youTubeArtTrackRevenueModel");
const YouTubePartnerChannelRevenue = require("../models/youTubePartnerChannelRevenueModel");
const YouTubeRDCChannelRevenue = require("../models/youTubeRDCChannelRevenueModel");
const YouTubeVideoClaimRevenue = require("../models/youTubeVideoClaimRevenueModel");
const YTPremiumRevenue = require("../models/ytPremiumRevenueModel");
const { excelSerialToISODate } = require("../utils/dateUtils");
const Release = require("../models/releaseModel");
const Contract = require("../models/contractModel");
const AudioStreamingReportHistory = require("../models/audioStreamingReportHistoryModel");
const YoutubeReportHistory = require("../models/youtubeReportHistoryModel");

const monthMap = {
    Jan: '01', Feb: '02', Mar: '03', Apr: '04',
    May: '05', Jun: '06', Jul: '07', Aug: '08',
    Sep: '09', Oct: '10', Nov: '11', Dec: '12'
};

const getDateFromMonthYear = (month, year) => {
    if (!month || !year) return null;
    const m = monthMap[month];
    return m ? `${year}-${m}-01` : null;
};

const BATCH_SIZE = 1000;


class revenueUploadController {

    constructor() {

        this.processPendingReports = this.processPendingReports.bind(this);
        this.processAudioStreamingReport = this.processAudioStreamingReport.bind(this);
        this.processPendingYoutubeReports = this.processPendingYoutubeReports.bind(this);
        this.processYoutubeReport = this.processYoutubeReport.bind(this);
        this.importRevenueFromJson = this.importRevenueFromJson.bind(this)
        this.insertBatch = this.insertBatch.bind(this)
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

            // Read Excel
            const workbook = XLSX.readFile(req.file.path);
            const sheet = workbook.Sheets[workbook.SheetNames[0]];
            const jsonData = XLSX.utils.sheet_to_json(sheet);

            if (jsonData.length === 0) {
                return res.status(400).json({ error: "Excel file is empty" });
            }

            // Process all rows and collect ISRC codes for batch lookup
            const rowsWithIsrc = [];
            const isrcCodes = new Set();

            // First pass: extract ISRC codes and prepare row data
            jsonData.forEach(r => {
                let isrcCode = null;
                let obj = {};

                // FACEBOOK MAPPING
                if (platform === "Facebook") {
                    isrcCode = r.elected_isrc;
                    obj = {
                        retailer: r.service || null,
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
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
                        date: excelSerialToISODate(r.Month) || null,
                    };

                    // SPOTIFY MAPPING
                } else if (platform === "Spotify") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: "Spotify",
                        label: r["Label Name"] || null,
                        upc_code: r.EAN || null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
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
                        date: excelSerialToISODate(r.Month) || null,
                    };

                    // Amazon MAPPING
                } else if (platform === "Amazon") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: "Amazon",
                        label: r["Label Name"] || null,
                        upc_code: r["Digital Album Upc"] || null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
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
                        date: excelSerialToISODate(r.Month) || null,
                    };

                    //JioSaavan MAPPING
                } else if (platform === "JioSaavan") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: "jio_savan",
                        label: r["Label Name"] || null,
                        upc_code: r.UPC || null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
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
                        date: excelSerialToISODate(r.Months) || null,
                    };

                    //AppleItunes MAPPING
                } else if (platform === "AppleItunes") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: "Apple Music",
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
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
                        date: excelSerialToISODate(r.Month) || null,
                    };

                    //TikTok MAPPING
                } else if (platform === "TikTok") {
                    isrcCode = r.Isrc;
                    obj = {
                        retailer: r.Platforn_Name,
                        label: r["Label Name"] || null,
                        upc_code: r.Product_Code,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r.Album || null,
                        track_title: r.Song_Title || null,
                        track_artist: r.Artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r.Views || null,
                        sale_type: null,
                        net_total: r["Total Revenue"] || null,
                        date: excelSerialToISODate(r.Months) || null,
                    };

                    //Gaana MAPPING
                } else if (platform === "Gaana") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: "Gaana",
                        label: r["Label Name"] || null,
                        upc_code: r["Album UPC"],
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
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
                        date: excelSerialToISODate(r.Month) || null,
                    };
                } else if (platform === "SoundRecording") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: r["Channel Name"] || null,
                        label: r["Label Name"] || null,
                        upc_code: r.UPC,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r.Album || null,
                        track_title: r["Asset Title"] || null,
                        track_artist: r.Artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: null,
                        track_count: r["Owned Views"] || null,
                        sale_type: null,
                        net_total: r["Total Revenue"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                    };
                } else if (platform === "YouTubeArtTrack") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: r["Channel Name"] || null,
                        label: r["Label Name"] || null,
                        upc_code: r.UPC,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r["Asset Title"] || null,
                        track_title: r["Asset Title"] || null,
                        track_artist: r.Artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: r["Content Type"] || null,
                        track_count: r["Owned Views"] || null,
                        sale_type: null,
                        net_total: r["Total Revenue"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                    };
                } else if (platform === "YouTubePartnerChannel") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: r["Channel Display Name"] || null,
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r["Asset Title"] || null,
                        track_title: r["Asset Title"] || null,
                        track_artist: r.Artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: r["Content Type"] || null,
                        track_count: r["Owned Views"] || null,
                        sale_type: null,
                        net_total: r["Total INR"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                    };
                } else if (platform === "YouTubeRDCChannel") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: r["Channel Display Name"] || null,
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r["Asset Title"] || null,
                        track_title: r["Asset Title"] || null,
                        track_artist: r.Artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: r["Content Type"] || null,
                        track_count: r["Owned Views"] || null,
                        sale_type: null,
                        net_total: r["Total INR"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                    };
                } else if (platform === "YouTubeVideoClaim") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: r["Channel Display Name"] || null,
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r["Asset Title"] || null,
                        track_title: r["Asset Title"] || null,
                        track_artist: r.Artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: r["Content Type"] || null,
                        track_count: r["Owned Views"] || null,
                        sale_type: null,
                        net_total: r["Total INR"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                    };
                } else if (platform === "YTPremiumRevenue") {
                    isrcCode = r.ISRC;
                    obj = {
                        retailer: r["Channel Display Name"] || null,
                        label: r["Label Name"] || null,
                        upc_code: null,
                        catalogue_number: null,
                        isrc_code: isrcCode || null,
                        release: r["Asset Title"] || null,
                        track_title: r["Asset Title"] || null,
                        track_artist: r.Artist || null,
                        remixer_name: null,
                        remix: null,
                        territory: r.Country || null,
                        purchase_status: null,
                        format: null,
                        delivery: "Streaming",
                        content_type: r["Content Type"] || null,
                        track_count: r["Owned Views"] || null,
                        sale_type: null,
                        net_total: r["Total INR"] || null,
                        date: excelSerialToISODate(r.Month) || null,
                    };
                }

                if (isrcCode) {
                    isrcCodes.add(isrcCode);
                }

                rowsWithIsrc.push({
                    data: obj,
                    isrc: isrcCode
                });
            });

            // Batch lookup for ISRC codes in Release model
            const isrcToLabelMap = {};
            if (isrcCodes.size > 0) {
                const releases = await Release.find({
                    isrc: { $in: Array.from(isrcCodes) },
                    deleted: 0
                }).select('isrc label_id');

                releases.forEach(release => {
                    isrcToLabelMap[release.isrc] = release.label_id;
                });
            }

            const RevenueUploads = await RevenueUpload.create({
                user_id: userId,
                platform,
                periodFrom: periodFrom || null,
                periodTo: periodTo || null,
                fileName: req.file.filename,
                filePath: fileURL,
                fileExt: req.file.mimetype,
            });

            // Prepare final mapped rows with label_id
            const mappedRows = [];

            rowsWithIsrc.forEach(row => {
                let labelId = 0;

                if (row.isrc && isrcToLabelMap[row.isrc]) {
                    labelId = isrcToLabelMap[row.isrc];
                }

                const today = new Date().toISOString().split("T")[0];
                const finalRow = {
                    ...row.data,
                    user_id: labelId,
                    uploading_date: today,
                    uploadId: RevenueUploads._id
                };

                mappedRows.push(finalRow);
            });

            const modelMap = {
                Spotify: SpotifyRevenue,
                AppleItunes: AppleRevenue,
                Gaana: GaanaRevenue,
                JioSaavan: JioSaavanRevenue,
                Facebook: FacebookRevenue,
                Amazon: AmazonRevenue,
                TikTok: TikTokRevenue,
                SoundRecording: SoundRecordingRevenue,
                YouTubeArtTrack: YouTubeArtTrackRevenue,
                YouTubePartnerChannel: YouTubePartnerChannelRevenue,
                YouTubeRDCChannel: YouTubeRDCChannelRevenue,
                YouTubeVideoClaim: YouTubeVideoClaimRevenue,
                YTPremiumRevenue: YTPremiumRevenue
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
                return res.status(400).json({
                    success: false,
                    message: "uploadId required"
                });
            }

            //  Accept the upload
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

            //  Fetch TempReport data
            const tempData = await TempReport.find({ uploadId }).lean();

            if (!tempData.length) {
                return res.status(404).json({
                    success: false,
                    message: "No data found for this uploadId"
                });
            }

            const cleanedData = tempData.map(({ _id, ...rest }) => rest);

            //  Collect unique user_ids
            const userIds = [...new Set(
                cleanedData
                    .map(r => r.user_id)
                    .filter(id => id !== null && id !== 0)
            )];

            //  Fetch active contracts
            const contracts = await Contract.find({
                user_id: { $in: userIds },
                status: "active"
            }).lean();

            //  Helper for safe date conversion
            const toDate = (d) => {
                if (!d) return null;
                const parsed = new Date(d);
                return isNaN(parsed) ? null : parsed;
            };

            //  Apply percentage to each row
            const finalData = cleanedData.map(row => {
                const rowDate = toDate(row.date);
                let percentage = 0;

                if (rowDate) {
                    const matchedContract = contracts.find(contract =>
                        contract.user_id === row.user_id &&
                        rowDate >= new Date(contract.startDate) &&
                        rowDate <= new Date(contract.endDate)
                    );

                    if (matchedContract) {
                        percentage = matchedContract.labelPercentage || 0;
                    }
                }

                return {
                    ...row,
                    percentage
                };
            });

            //  Insert into TblReport2025
            await TblReport2025.insertMany(finalData);

            //  Clear TempReport
            await TempReport.deleteMany({ uploadId });

            // Logging
            await LogService.createLog({
                user_id: userId,
                email,
                action: `REVENUE_ADDED_IN_TBLREPORT_FOR_${tempData[0].retailer}`,
                description: `${tempData[0].retailer} revenue uploaded successfully in tbl_report`,
                newData: finalData,
                req
            });

            return res.status(200).json({
                success: true,
                message: "Data moved from TempReport to TblReport_2025 successfully",
                insertedCount: finalData.length
            });

        } catch (error) {
            console.error("uploadTblRevenue error:", error);
            return res.status(500).json({
                success: false,
                message: "Something went wrong",
                error: error.message
            });
        }
    }

    // getAudioStreamingRevenueSummary method
    async getAudioStreamingRevenueSummary(req, res, next) {
        try {
            const {
                labelId, platform, year, month, fromDate, toDate,
                releases, artist, track, territory
            } = req.query;

            const { role, userId } = req.user;

            // === Build filter (same as original) ===
            const filter = {};
            if (role && role !== "Super Admin" && role !== "Manager") {
                const users = await User.find({ parent_id: userId }, { id: 1 }).lean();
                const childIds = users.map(u => u.id);
                childIds.push(userId);
                filter.user_id = { $in: childIds };
            }
            if (labelId) filter.user_id = Number(labelId);

            const defaultRetailers = ["Apple Music", "Spotify", "Gaana", "Jio Saavn", "Facebook", "Amazon", "TikTok"];
            if (platform && platform !== "") {
                filter.retailer = { $in: platform.split(",").map(p => p.trim()) };
            } else {
                filter.retailer = { $in: defaultRetailers };
            }

            const selectedYear = year ? parseInt(year) : new Date().getFullYear();
            if (year && !month && !fromDate && !toDate) {
                filter.date = { $gte: `${selectedYear}-01-01`, $lte: `${selectedYear}-12-31` };
            }
            if (month) {
                const start = new Date(selectedYear, parseInt(month) - 1, 1);
                const end = new Date(selectedYear, parseInt(month), 0);
                filter.date = { $gte: start.toISOString().split("T")[0], $lte: end.toISOString().split("T")[0] };
            }
            if (fromDate && toDate) {
                const [fy, fm] = fromDate.split("-").map(Number);
                const [ty, tm] = toDate.split("-").map(Number);
                filter.date = {
                    $gte: new Date(fy, fm - 1, 1).toISOString().split("T")[0],
                    $lte: new Date(ty, tm, 0).toISOString().split("T")[0]
                };
            }

            if (artist === "true") filter.track_artist = { $nin: ["", null, undefined] };
            if (territory === "true") filter.territory = { $nin: ["", null, undefined] };
            if (releases === "true") filter.release = { $nin: ["", null, undefined] };

            // === Step 1: Daily aggregation for revenue and streams ===
            const dailyPipeline = [
                { $match: filter },
                {
                    $addFields: {
                        revenueNum: { $convert: { input: "$net_total", to: "double", onError: 0, onNull: 0 } },
                        streamsNum: { $convert: { input: "$track_count", to: "long", onError: 0, onNull: 0 } }
                    }
                },
                {
                    $group: {
                        _id: { user_id: "$user_id", date: "$date" },
                        dailyRevenue: { $sum: "$revenueNum" },
                        dailyStreams: { $sum: "$streamsNum" }
                    }
                },
                {
                    $project: {
                        user_id: "$_id.user_id",
                        date: "$_id.date",
                        dailyRevenue: 1,
                        dailyStreams: 1,
                        _id: 0
                    }
                }
            ];

            const dailyData = await TblReport2025.aggregate(dailyPipeline).allowDiskUse(true);

            // === Step 2: Fetch contracts and apply deductions ===
            const uniqueUserIds = [...new Set(dailyData.map(d => d.user_id).filter(Boolean))];
            const contracts = uniqueUserIds.length > 0
                ? await Contract.find({ user_id: { $in: uniqueUserIds }, status: "active" }).lean()
                : [];

            const contractMap = new Map();
            contracts.forEach(c => {
                if (!contractMap.has(c.user_id)) contractMap.set(c.user_id, []);
                contractMap.get(c.user_id).push(c);
            });

            // Apply deductions
            let totalDeductedRevenue = 0;
            let totalStreams = 0;
            let entriesWithDeduction = 0;
            let sumDeductionPercent = 0;

            dailyData.forEach(item => {
                let deducted = item.dailyRevenue;
                let percentage = 0;
                let applied = false;

                const userContracts = contractMap.get(item.user_id) || [];
                for (const contract of userContracts) {
                    if (item.date >= contract.startDate && item.date <= contract.endDate) {
                        percentage = contract.labelPercentage || 0;
                        deducted = item.dailyRevenue * ((100 - percentage) / 100);
                        applied = true;
                        break;
                    }
                }

                if (applied) {
                    entriesWithDeduction++;
                    sumDeductionPercent += percentage;
                }

                totalDeductedRevenue += deducted;
                totalStreams += item.dailyStreams;
            });

            const avgDeductionPercentage = entriesWithDeduction > 0 ? sumDeductionPercent / entriesWithDeduction : 0;

            // === Step 3: Charts data ===
            const chartPipeline = [
                { $match: filter },
                {
                    $addFields: {
                        revenueNum: { $convert: { input: "$net_total", to: "double", onError: 0, onNull: 0 } }
                    }
                },
                {
                    $facet: {
                        byMonth: [
                            { $group: { _id: { $dateToString: { format: "%b %Y", date: { $dateFromString: { dateString: "$date" } } } }, revenue: { $sum: "$revenueNum" } } },
                            { $sort: { _id: 1 } },
                            { $project: { month: "$_id", revenue: { $round: ["$revenue", 2] }, _id: 0 } }
                        ],
                        byPlatform: [
                            { $group: { _id: { $ifNull: ["$retailer", "Unknown"] }, revenue: { $sum: "$revenueNum" } } },
                            { $sort: { revenue: -1 } },
                            { $project: { platform: "$_id", revenue: { $round: ["$revenue", 2] }, _id: 0 } }
                        ],
                        byCountry: [
                            { $group: { _id: { $ifNull: ["$territory", "Unknown"] }, revenue: { $sum: "$revenueNum" } } },
                            { $sort: { revenue: -1 } },
                            { $limit: 10 },
                            { $project: { country: "$_id", revenue: { $round: ["$revenue", 2] }, _id: 0 } }
                        ]
                    }
                }
            ];

            const [chartResult] = await TblReport2025.aggregate(chartPipeline).allowDiskUse(true);

            // Apply global deduction ratio to charts
            const grossTotal = chartResult.byMonth.reduce((s, m) => s + m.revenue, 0);
            const deductionRatio = grossTotal > 0 ? totalDeductedRevenue / grossTotal : 1;

            const revenueByMonth = Object.fromEntries(
                chartResult.byMonth.map(m => [m.month, Number((m.revenue * deductionRatio).toFixed(2))])
            );
            const revenueByChannel = Object.fromEntries(
                chartResult.byPlatform.map(p => [p.platform, Number((p.revenue * deductionRatio).toFixed(2))])
            );
            const revenueByCountry = Object.fromEntries(
                chartResult.byCountry.map(c => [c.country, Number((c.revenue * deductionRatio).toFixed(2))])
            );

            // === Response ===
            res.json({
                success: true,
                data: {
                    summary: {
                        totalStreams: totalStreams || 0,
                        totalRevenue: Number(totalDeductedRevenue.toFixed(2)),
                        deductionApplied: entriesWithDeduction > 0,
                        deductionPercentage: Number(avgDeductionPercentage.toFixed(2)),
                        entriesWithDeduction,
                        totalEntries: dailyData.length
                    },
                    revenueByMonth,
                    revenueByChannel,
                    revenueByCountry
                }
            });

        } catch (error) {
            console.error("Error in getAudioStreamingRevenueSummary:", error);
            next(error);
        }
    }

    // getAudioStreamingRevenueReports method
    async getAudioStreamingRevenueReports(req, res, next) {
        try {
            const {
                labelId, platform, year, month, fromDate, toDate,
                releases, artist, track, territory,
                page = 1, limit = 10
            } = req.query;

            const { role, userId } = req.user;

            // === Build filter (same as original) ===
            const filter = {};
            if (role && role !== "Super Admin" && role !== "Manager") {
                const users = await User.find({ parent_id: userId }, { id: 1 }).lean();
                const childIds = users.map(u => u.id);
                childIds.push(userId);
                filter.user_id = { $in: childIds };
            }
            if (labelId) filter.user_id = Number(labelId);

            const defaultRetailers = ["Apple Music", "Spotify", "Gaana", "Jio Saavn", "Facebook", "Amazon", "TikTok"];
            if (platform && platform !== "") {
                filter.retailer = { $in: platform.split(",").map(p => p.trim()) };
            } else {
                filter.retailer = { $in: defaultRetailers };
            }

            const selectedYear = year ? parseInt(year) : new Date().getFullYear();
            if (year && !month && !fromDate && !toDate) {
                filter.date = { $gte: `${selectedYear}-01-01`, $lte: `${selectedYear}-12-31` };
            }
            if (month) {
                const start = new Date(selectedYear, parseInt(month) - 1, 1);
                const end = new Date(selectedYear, parseInt(month), 0);
                filter.date = { $gte: start.toISOString().split("T")[0], $lte: end.toISOString().split("T")[0] };
            }
            if (fromDate && toDate) {
                const [fy, fm] = fromDate.split("-").map(Number);
                const [ty, tm] = toDate.split("-").map(Number);
                filter.date = {
                    $gte: new Date(fy, fm - 1, 1).toISOString().split("T")[0],
                    $lte: new Date(ty, tm, 0).toISOString().split("T")[0]
                };
            }

            if (artist === "true") filter.track_artist = { $nin: ["", null, undefined] };
            if (territory === "true") filter.territory = { $nin: ["", null, undefined] };
            if (releases === "true") filter.release = { $nin: ["", null, undefined] };

            const pageNum = parseInt(page);
            const limitNum = parseInt(limit);
            const skipNum = (pageNum - 1) * limitNum;

            // === Step 1: Daily aggregation for deduction calculation ===
            const dailyPipeline = [
                { $match: filter },
                {
                    $addFields: {
                        revenueNum: { $convert: { input: "$net_total", to: "double", onError: 0, onNull: 0 } }
                    }
                },
                {
                    $group: {
                        _id: { user_id: "$user_id", date: "$date" },
                        dailyRevenue: { $sum: "$revenueNum" }
                    }
                },
                {
                    $project: {
                        user_id: "$_id.user_id",
                        date: "$_id.date",
                        dailyRevenue: 1,
                        _id: 0
                    }
                }
            ];

            const dailyData = await TblReport2025.aggregate(dailyPipeline).allowDiskUse(true);

            // === Step 2: Fetch contracts ===
            const uniqueUserIds = [...new Set(dailyData.map(d => d.user_id).filter(Boolean))];
            const contracts = uniqueUserIds.length > 0
                ? await Contract.find({ user_id: { $in: uniqueUserIds }, status: "active" }).lean()
                : [];

            const contractMap = new Map();
            contracts.forEach(c => {
                if (!contractMap.has(c.user_id)) contractMap.set(c.user_id, []);
                contractMap.get(c.user_id).push(c);
            });

            // Apply deductions to daily data
            const dailyDeducted = dailyData.map(item => {
                let deducted = item.dailyRevenue;

                const userContracts = contractMap.get(item.user_id) || [];
                for (const contract of userContracts) {
                    if (item.date >= contract.startDate && item.date <= contract.endDate) {
                        const percentage = contract.labelPercentage || 0;
                        deducted = item.dailyRevenue * ((100 - percentage) / 100);
                        break;
                    }
                }

                return { date: item.date, user_id: item.user_id, deductedRevenue: deducted };
            });

            // === Step 3: Artist Report ===
            const artistPipeline = [
                { $match: filter },
                {
                    $addFields: {
                        revenueNum: { $convert: { input: "$net_total", to: "double", onError: 0, onNull: 0 } }
                    }
                },
                {
                    $group: {
                        _id: {
                            artist: { $ifNull: ["$track_artist", "Unknown Artist"] },
                            user_id: "$user_id"
                        },
                        grossRevenue: { $sum: "$revenueNum" },
                        sampleDate: { $first: "$date" },
                        samplePlatform: { $first: "$retailer" },
                        sampleRelease: { $first: "$release" }
                    }
                }
            ];

            const artistData = await TblReport2025.aggregate(artistPipeline).allowDiskUse(true);

            // Apply deduction proportionally per artist
            const artistReports = artistData.map(item => {
                const userDaily = dailyDeducted.filter(d => d.user_id === item._id.user_id);
                const totalGrossForUser = userDaily.reduce((s, d) => s + (dailyData.find(dd => dd.user_id === item._id.user_id && dd.date === d.date)?.dailyRevenue || 0), 0);
                const totalDeductedForUser = userDaily.reduce((s, d) => s + d.deductedRevenue, 0);

                const deductionRatio = totalGrossForUser > 0 ? totalDeductedForUser / totalGrossForUser : 1;

                return {
                    artist: item._id.artist,
                    revenue: Number((item.grossRevenue * deductionRatio).toFixed(2)),
                    date: item.sampleDate,
                    platform: item.samplePlatform || "Various",
                    release: item.sampleRelease || "Various",
                    user_id: item._id.user_id
                };
            })
                .sort((a, b) => b.revenue - a.revenue);

            const totalRecords = artistReports.length;
            const paginatedReports = artistReports.slice(skipNum, skipNum + limitNum);

            // === Response ===
            res.json({
                success: true,
                data: {
                    reports: paginatedReports,
                    pagination: {
                        totalRecords,
                        totalPages: Math.ceil(totalRecords / limitNum),
                        currentPage: pageNum,
                        limit: limitNum
                    }
                }
            });

        } catch (error) {
            console.error("Error in getAudioStreamingRevenueReports:", error);
            next(error);
        }
    }

    // getYoutubeRevenueSummary method
    async getYoutubeRevenueSummary(req, res, next) {
        try {
            const {
                labelId, platform, year, month, fromDate, toDate,
                releases, artist, track, territory
            } = req.query;

            const { role, userId } = req.user;

            // === Build filter (same as original) ===
            const filter = {};
            if (role && role !== "Super Admin" && role !== "Manager") {
                const users = await User.find({ parent_id: userId }, { id: 1 }).lean();
                const childIds = users.map(u => u.id);
                childIds.push(userId);
                filter.user_id = { $in: childIds };
            }
            if (labelId) filter.user_id = Number(labelId);

            const defaultRetailers = [
                "Sound Recording (Audio Claim)",
                "Art Track (YouTube Music)",
                "YouTubePartnerChannel",
                "YouTubeRDCChannel",
                "YouTubeVideoClaim",
                "YTPremiumRevenue",
            ];
            if (platform && platform !== "") {
                filter.retailer = { $in: platform.split(",").map(p => p.trim()) };
            } else {
                filter.retailer = { $in: defaultRetailers };
            }

            const selectedYear = year ? parseInt(year) : new Date().getFullYear();
            if (year && !month && !fromDate && !toDate) {
                filter.date = { $gte: `${selectedYear}-01-01`, $lte: `${selectedYear}-12-31` };
            }
            if (month) {
                const start = new Date(selectedYear, parseInt(month) - 1, 1);
                const end = new Date(selectedYear, parseInt(month), 0);
                filter.date = { $gte: start.toISOString().split("T")[0], $lte: end.toISOString().split("T")[0] };
            }
            if (fromDate && toDate) {
                const [fy, fm] = fromDate.split("-").map(Number);
                const [ty, tm] = toDate.split("-").map(Number);
                filter.date = {
                    $gte: new Date(fy, fm - 1, 1).toISOString().split("T")[0],
                    $lte: new Date(ty, tm, 0).toISOString().split("T")[0]
                };
            }

            if (artist === "true") filter.track_artist = { $nin: ["", null, undefined] };
            if (territory === "true") filter.territory = { $nin: ["", null, undefined] };
            if (releases === "true") filter.release = { $nin: ["", null, undefined] };

            // === Step 1: Daily aggregation for revenue and streams ===
            const dailyPipeline = [
                { $match: filter },
                {
                    $addFields: {
                        revenueNum: { $convert: { input: "$net_total", to: "double", onError: 0, onNull: 0 } },
                        streamsNum: { $convert: { input: "$track_count", to: "long", onError: 0, onNull: 0 } }
                    }
                },
                {
                    $group: {
                        _id: { user_id: "$user_id", date: "$date" },
                        dailyRevenue: { $sum: "$revenueNum" },
                        dailyStreams: { $sum: "$streamsNum" }
                    }
                },
                {
                    $project: {
                        user_id: "$_id.user_id",
                        date: "$_id.date",
                        dailyRevenue: 1,
                        dailyStreams: 1,
                        _id: 0
                    }
                }
            ];

            const dailyData = await TblReport2025.aggregate(dailyPipeline).allowDiskUse(true);

            // === Step 2: Fetch contracts and apply deductions ===
            const uniqueUserIds = [...new Set(dailyData.map(d => d.user_id).filter(Boolean))];
            const contracts = uniqueUserIds.length > 0
                ? await Contract.find({ user_id: { $in: uniqueUserIds }, status: "active" }).lean()
                : [];

            const contractMap = new Map();
            contracts.forEach(c => {
                if (!contractMap.has(c.user_id)) contractMap.set(c.user_id, []);
                contractMap.get(c.user_id).push(c);
            });

            // Apply deductions
            let totalDeductedRevenue = 0;
            let totalStreams = 0;
            let entriesWithDeduction = 0;
            let sumDeductionPercent = 0;

            dailyData.forEach(item => {
                let deducted = item.dailyRevenue;
                let percentage = 0;
                let applied = false;

                const userContracts = contractMap.get(item.user_id) || [];
                for (const contract of userContracts) {
                    if (item.date >= contract.startDate && item.date <= contract.endDate) {
                        percentage = contract.labelPercentage || 0;
                        deducted = item.dailyRevenue * ((100 - percentage) / 100);
                        applied = true;
                        break;
                    }
                }

                if (applied) {
                    entriesWithDeduction++;
                    sumDeductionPercent += percentage;
                }

                totalDeductedRevenue += deducted;
                totalStreams += item.dailyStreams;
            });

            const avgDeductionPercentage = entriesWithDeduction > 0 ? sumDeductionPercent / entriesWithDeduction : 0;

            // === Step 3: Charts data ===
            const chartPipeline = [
                { $match: filter },
                {
                    $addFields: {
                        revenueNum: { $convert: { input: "$net_total", to: "double", onError: 0, onNull: 0 } }
                    }
                },
                {
                    $facet: {
                        byMonth: [
                            { $group: { _id: { $dateToString: { format: "%b %Y", date: { $dateFromString: { dateString: "$date" } } } }, revenue: { $sum: "$revenueNum" } } },
                            { $sort: { _id: 1 } },
                            { $project: { month: "$_id", revenue: { $round: ["$revenue", 2] }, _id: 0 } }
                        ],
                        byPlatform: [
                            { $group: { _id: { $ifNull: ["$retailer", "Unknown"] }, revenue: { $sum: "$revenueNum" } } },
                            { $sort: { revenue: -1 } },
                            { $project: { platform: "$_id", revenue: { $round: ["$revenue", 2] }, _id: 0 } }
                        ],
                        byCountry: [
                            { $group: { _id: { $ifNull: ["$territory", "Unknown"] }, revenue: { $sum: "$revenueNum" } } },
                            { $sort: { revenue: -1 } },
                            { $limit: 10 },
                            { $project: { country: "$_id", revenue: { $round: ["$revenue", 2] }, _id: 0 } }
                        ]
                    }
                }
            ];

            const [chartResult] = await TblReport2025.aggregate(chartPipeline).allowDiskUse(true);

            // Apply global deduction ratio to charts
            const grossTotal = chartResult.byMonth.reduce((s, m) => s + m.revenue, 0);
            const deductionRatio = grossTotal > 0 ? totalDeductedRevenue / grossTotal : 1;

            const revenueByMonth = Object.fromEntries(
                chartResult.byMonth.map(m => [m.month, Number((m.revenue * deductionRatio).toFixed(2))])
            );
            const revenueByChannel = Object.fromEntries(
                chartResult.byPlatform.map(p => [p.platform, Number((p.revenue * deductionRatio).toFixed(2))])
            );
            const revenueByCountry = Object.fromEntries(
                chartResult.byCountry.map(c => [c.country, Number((c.revenue * deductionRatio).toFixed(2))])
            );

            // === Response ===
            res.json({
                success: true,
                data: {
                    summary: {
                        totalStreams: totalStreams || 0,
                        totalRevenue: Number(totalDeductedRevenue.toFixed(2)),
                        deductionApplied: entriesWithDeduction > 0,
                        deductionPercentage: Number(avgDeductionPercentage.toFixed(2)),
                        entriesWithDeduction,
                        totalEntries: dailyData.length
                    },
                    revenueByMonth,
                    revenueByChannel,
                    revenueByCountry
                }
            });

        } catch (error) {
            console.error("Error in getAudioStreamingRevenueSummary:", error);
            next(error);
        }
    }

    // getYoutubeRevenueReports method
    async getYoutubeRevenueReports(req, res, next) {
        try {
            const {
                labelId, platform, year, month, fromDate, toDate,
                releases, artist, track, territory,
                page = 1, limit = 10
            } = req.query;

            const { role, userId } = req.user;

            // === Build filter (same as original) ===
            const filter = {};
            if (role && role !== "Super Admin" && role !== "Manager") {
                const users = await User.find({ parent_id: userId }, { id: 1 }).lean();
                const childIds = users.map(u => u.id);
                childIds.push(userId);
                filter.user_id = { $in: childIds };
            }
            if (labelId) filter.user_id = Number(labelId);

            const defaultRetailers = [
                "Sound Recording (Audio Claim)",
                "Art Track (YouTube Music)",
                "YouTubePartnerChannel",
                "YouTubeRDCChannel",
                "YouTubeVideoClaim",
                "YTPremiumRevenue",
            ];
            if (platform && platform !== "") {
                filter.retailer = { $in: platform.split(",").map(p => p.trim()) };
            } else {
                filter.retailer = { $in: defaultRetailers };
            }

            const selectedYear = year ? parseInt(year) : new Date().getFullYear();
            if (year && !month && !fromDate && !toDate) {
                filter.date = { $gte: `${selectedYear}-01-01`, $lte: `${selectedYear}-12-31` };
            }
            if (month) {
                const start = new Date(selectedYear, parseInt(month) - 1, 1);
                const end = new Date(selectedYear, parseInt(month), 0);
                filter.date = { $gte: start.toISOString().split("T")[0], $lte: end.toISOString().split("T")[0] };
            }
            if (fromDate && toDate) {
                const [fy, fm] = fromDate.split("-").map(Number);
                const [ty, tm] = toDate.split("-").map(Number);
                filter.date = {
                    $gte: new Date(fy, fm - 1, 1).toISOString().split("T")[0],
                    $lte: new Date(ty, tm, 0).toISOString().split("T")[0]
                };
            }

            if (artist === "true") filter.track_artist = { $nin: ["", null, undefined] };
            if (territory === "true") filter.territory = { $nin: ["", null, undefined] };
            if (releases === "true") filter.release = { $nin: ["", null, undefined] };

            const pageNum = parseInt(page);
            const limitNum = parseInt(limit);
            const skipNum = (pageNum - 1) * limitNum;

            // === Step 1: Daily aggregation for deduction calculation ===
            const dailyPipeline = [
                { $match: filter },
                {
                    $addFields: {
                        revenueNum: { $convert: { input: "$net_total", to: "double", onError: 0, onNull: 0 } }
                    }
                },
                {
                    $group: {
                        _id: { user_id: "$user_id", date: "$date" },
                        dailyRevenue: { $sum: "$revenueNum" }
                    }
                },
                {
                    $project: {
                        user_id: "$_id.user_id",
                        date: "$_id.date",
                        dailyRevenue: 1,
                        _id: 0
                    }
                }
            ];

            const dailyData = await TblReport2025.aggregate(dailyPipeline).allowDiskUse(true);

            // === Step 2: Fetch contracts ===
            const uniqueUserIds = [...new Set(dailyData.map(d => d.user_id).filter(Boolean))];
            const contracts = uniqueUserIds.length > 0
                ? await Contract.find({ user_id: { $in: uniqueUserIds }, status: "active" }).lean()
                : [];

            const contractMap = new Map();
            contracts.forEach(c => {
                if (!contractMap.has(c.user_id)) contractMap.set(c.user_id, []);
                contractMap.get(c.user_id).push(c);
            });

            // Apply deductions to daily data
            const dailyDeducted = dailyData.map(item => {
                let deducted = item.dailyRevenue;

                const userContracts = contractMap.get(item.user_id) || [];
                for (const contract of userContracts) {
                    if (item.date >= contract.startDate && item.date <= contract.endDate) {
                        const percentage = contract.labelPercentage || 0;
                        deducted = item.dailyRevenue * ((100 - percentage) / 100);
                        break;
                    }
                }

                return { date: item.date, user_id: item.user_id, deductedRevenue: deducted };
            });

            // === Step 3: Artist Report ===
            const artistPipeline = [
                { $match: filter },
                {
                    $addFields: {
                        revenueNum: { $convert: { input: "$net_total", to: "double", onError: 0, onNull: 0 } }
                    }
                },
                {
                    $group: {
                        _id: {
                            artist: { $ifNull: ["$track_artist", "Unknown Artist"] },
                            user_id: "$user_id"
                        },
                        grossRevenue: { $sum: "$revenueNum" },
                        sampleDate: { $first: "$date" },
                        samplePlatform: { $first: "$retailer" },
                        sampleRelease: { $first: "$release" }
                    }
                }
            ];

            const artistData = await TblReport2025.aggregate(artistPipeline).allowDiskUse(true);

            // Apply deduction proportionally per artist
            const artistReports = artistData.map(item => {
                const userDaily = dailyDeducted.filter(d => d.user_id === item._id.user_id);
                const totalGrossForUser = userDaily.reduce((s, d) => s + (dailyData.find(dd => dd.user_id === item._id.user_id && dd.date === d.date)?.dailyRevenue || 0), 0);
                const totalDeductedForUser = userDaily.reduce((s, d) => s + d.deductedRevenue, 0);

                const deductionRatio = totalGrossForUser > 0 ? totalDeductedForUser / totalGrossForUser : 1;

                return {
                    artist: item._id.artist,
                    revenue: Number((item.grossRevenue * deductionRatio).toFixed(2)),
                    date: item.sampleDate,
                    platform: item.samplePlatform || "Various",
                    release: item.sampleRelease || "Various",
                    user_id: item._id.user_id
                };
            })
                .sort((a, b) => b.revenue - a.revenue);

            const totalRecords = artistReports.length;
            const paginatedReports = artistReports.slice(skipNum, skipNum + limitNum);

            // === Response ===
            res.json({
                success: true,
                data: {
                    reports: paginatedReports,
                    pagination: {
                        totalRecords,
                        totalPages: Math.ceil(totalRecords / limitNum),
                        currentPage: pageNum,
                        limit: limitNum
                    }
                }
            });

        } catch (error) {
            console.error("Error in getAudioStreamingRevenueReports:", error);
            next(error);
        }
    }

    // downloadExcelReport method
    async triggerAudioStreamingExcelReport(req, res, next) {
        try {
            const { userId } = req.user;

            const existingReport = await AudioStreamingReportHistory.findOne({
                'filters': req.query,
                status: 'pending'
            });

            if (existingReport) {
                return res.status(200).json({
                    success: true,
                    message: "Report is already being prepared",
                    reportId: existingReport._id
                });
            }

            // Create report with "pending" status
            const newReport = new AudioStreamingReportHistory({
                user_id: userId,
                filters: req.query,
                status: 'pending',
                generatedAt: new Date(),
                filename: 'Generating...'
            });

            await newReport.save();

            return res.status(200).json({
                success: true,
                message: "Report generation started",
                reportId: newReport._id
            });

        } catch (error) {
            console.error("Error triggering report:", error);
            return res.status(500).json({
                success: false,
                message: "Error starting report generation",
                error: error.message
            });
        }
    }

    // Separate function to process the report
    async processAudioStreamingReport(reportId, filters) {
        try {
            console.log(`Processing report ${reportId} with filters:`, filters);

            const {
                userId,
                role,
                labelId,
                platform,
                year,
                month,
                fromDate,
                toDate,
                releases,
                artist,
                track,
                partner,
                contentType,
                format,
                territory,
                quarters
            } = filters;

            const userFilter = {};

            if (userId && role) {
                if (role !== "Super Admin" && role !== "Manager") {
                    // For non-Super Admin/Manager users, get child users
                    const users = await User.find({ parent_id: userId }, { id: 1 });
                    const childIds = users.map(u => u.id);
                    childIds.push(userId);
                    userFilter.user_id = { $in: childIds };
                }
            }

            const defaultRetailers = [
                "Apple Music",
                "Spotify",
                "Gaana",
                "Jio Saavn",
                "Facebook",
                "Amazon",
                "TikTok"
            ];

            const filter = { ...userFilter };

            if (labelId) {
                filter.user_id = Number(labelId);
            }

            if (platform && platform !== "") {
                const platforms = platform.split(",").map(p => p.trim());
                filter.retailer = { $in: platforms };
            } else {
                filter.retailer = { $in: defaultRetailers };
            }

            const selectedYear = year ? parseInt(year) : new Date().getFullYear();

            if (year && !month && !fromDate && !toDate) {
                filter.date = {
                    $gte: `${selectedYear}-01-01`,
                    $lte: `${selectedYear}-12-31`
                };
            }

            if (month && month !== '') {
                const startDate = new Date(selectedYear, parseInt(month) - 1, 1);
                const endDate = new Date(selectedYear, parseInt(month), 0);
                filter.date = {
                    $gte: startDate.toISOString().split("T")[0],
                    $lte: endDate.toISOString().split("T")[0]
                };
            }

            if (fromDate && toDate) {
                const [fromYear, fromMonth] = fromDate.split("-").map(Number);
                const [toYear, toMonth] = toDate.split("-").map(Number);

                const startDate = new Date(fromYear, fromMonth - 1, 1);
                const endDate = new Date(toYear, toMonth, 0);

                filter.date = {
                    $gte: startDate.toISOString().split("T")[0],
                    $lte: endDate.toISOString().split("T")[0]
                };
            }

            if (artist === "true") filter.track_artist = { $nin: ["", null, undefined] };
            if (territory === "true") filter.territory = { $nin: ["", null, undefined] };
            if (releases === "true") filter.release = { $nin: ["", null, undefined] };

            const pipeline = [
                { $match: filter },
                { $sort: { date: -1 } },
                { $project: { __v: 0, createdAt: 0, updatedAt: 0 } }
            ];

            console.log(`Filter for report ${reportId}:`, JSON.stringify(filter, null, 2));

            const count = await TblReport2025.countDocuments(filter);
            console.log(`Total records found for report ${reportId}: ${count}`);

            if (count === 0) {
                await AudioStreamingReportHistory.findByIdAndUpdate(reportId, {
                    status: 'failed',
                    error: 'No data found',
                });
                return;
            }

            // NEW: Create file path early
            const timestamp = new Date().toISOString().split('T')[0].replace(/-/g, '');
            const randomSuffix = Math.random().toString(36).substring(2, 8);
            const filename = `Revenue_Report_${timestamp}_${randomSuffix}.csv`;

            const relativeFolder = 'reports';
            const absoluteFolder = path.join(__dirname, '../uploads', relativeFolder);

            if (!fs.existsSync(absoluteFolder)) {
                fs.mkdirSync(absoluteFolder, { recursive: true });
            }

            const absoluteFilePath = path.join(absoluteFolder, filename);
            const relativePath = `uploads/reports/${filename}`;
            const fileURL = `${process.env.BASE_URL}/${relativePath}`;

            // NEW: Streaming approach – no more huge "data" array!
            const writeStream = fs.createWriteStream(absoluteFilePath);

            // We'll write headers after getting the first document (dynamic headers)
            let headersWritten = false;
            let headers = ["S.No"];
            let rowIndex = 1;

            const excludeFields = ["_id", "__v", "createdAt", "updatedAt"];

            // Use aggregation cursor for true streaming (low memory)
            const collection = mongoose.connection.db.collection('tblreport_2025');
            const cursor = collection.aggregate(pipeline, {
                allowDiskUse: true,
                cursor: { batchSize: 1000 } // Adjust batch size if needed
            });

            // Create a fast-csv formatter that writes directly to file
            const csvStream = fastCsv.format({ headers: false, includeEndRowDelimiter: true });

            csvStream.pipe(writeStream);

            let firstDoc = true;

            for await (const doc of cursor) {
                if (firstDoc) {
                    // Build headers from first document (same as your original code)
                    Object.keys(doc).forEach(key => {
                        if (!excludeFields.includes(key) && key !== "date") {
                            headers.push(key);
                        }
                    });
                    headers.push("date");

                    // Write header row
                    csvStream.write(headers);
                    headersWritten = true;
                    firstDoc = false;
                }

                // Build row data
                const rowData = [rowIndex++];

                Object.keys(doc).forEach(key => {
                    if (!excludeFields.includes(key) && key !== "date") {
                        rowData.push(doc[key] ?? "");
                    }
                });
                rowData.push(doc.date ?? "");

                csvStream.write(rowData);

                // Optional: log progress
                if (rowIndex % 10000 === 0) {
                    console.log(`Streamed ${rowIndex} rows...`);
                }
            }

            // End the streams
            csvStream.end();
            cursor.close();

            // Wait for file to finish writing
            await new Promise((resolve, reject) => {
                writeStream.on('finish', resolve);
                writeStream.on('error', reject);
            });

            console.log(`CSV report saved: ${absoluteFilePath} (${rowIndex - 1} rows)`);

            // Update history
            await AudioStreamingReportHistory.findByIdAndUpdate(reportId, {
                status: 'ready',
                filename,
                filePath: relativePath,
                fileURL,
            });

            console.log(`Report ${reportId} successfully generated as CSV`);

        } catch (error) {
            console.error(`Error processing report ${reportId}:`, error);
            await AudioStreamingReportHistory.findByIdAndUpdate(reportId, {
                status: 'failed',
                error: error.message || 'Unknown error',
            });
            throw error;
        }
    }

    // Trigger YouTube report generation
    async triggerYoutubeExcelReport(req, res, next) {
        try {
            const { userId } = req.user;

            const existingReport = await YoutubeReportHistory.findOne({
                'filters': req.query,
                status: 'pending'
            });

            if (existingReport) {
                return res.status(200).json({
                    success: true,
                    message: "YouTube report is already being prepared",
                    reportId: existingReport._id
                });
            }

            // Create report with "pending" status
            const newReport = new YoutubeReportHistory({
                user_id: userId,
                filters: req.query,
                status: 'pending',
                generatedAt: new Date(),
                filename: 'Generating...'
            });

            await newReport.save();

            return res.status(200).json({
                success: true,
                message: "YouTube report generation started",
                reportId: newReport._id
            });

        } catch (error) {
            console.error("Error triggering YouTube report:", error);
            return res.status(500).json({
                success: false,
                message: "Error starting YouTube report generation",
                error: error.message
            });
        }
    }

    // Process YouTube report
    async processYoutubeReport(reportId, filters) {
        try {
            console.log(`Processing report ${reportId} with filters:`, filters);

            const {
                userId,
                role,
                labelId,
                platform,
                year,
                month,
                fromDate,
                toDate,
                releases,
                artist,
                track,
                partner,
                contentType,
                format,
                territory,
                quarters
            } = filters;

            const userFilter = {};

            if (userId && role) {
                if (role !== "Super Admin" && role !== "Manager") {
                    // For non-Super Admin/Manager users, get child users
                    const users = await User.find({ parent_id: userId }, { id: 1 });
                    const childIds = users.map(u => u.id);
                    childIds.push(userId);
                    userFilter.user_id = { $in: childIds };
                }
            }

            const defaultRetailers = [
                "Sound Recording (Audio Claim)",
                "Art Track (YouTube Music)",
                "YouTubePartnerChannel",
                "YouTubeRDCChannel",
                "YouTubeVideoClaim",
                "YTPremiumRevenue",
            ];

            const filter = { ...userFilter };

            if (labelId) {
                filter.user_id = Number(labelId);
            }

            if (platform && platform !== "") {
                const platforms = platform.split(",").map(p => p.trim());
                filter.retailer = { $in: platforms };
            } else {
                filter.retailer = { $in: defaultRetailers };
            }

            const selectedYear = year ? parseInt(year) : new Date().getFullYear();

            if (year && !month && !fromDate && !toDate) {
                filter.date = {
                    $gte: `${selectedYear}-01-01`,
                    $lte: `${selectedYear}-12-31`
                };
            }

            if (month && month !== '') {
                const startDate = new Date(selectedYear, parseInt(month) - 1, 1);
                const endDate = new Date(selectedYear, parseInt(month), 0);
                filter.date = {
                    $gte: startDate.toISOString().split("T")[0],
                    $lte: endDate.toISOString().split("T")[0]
                };
            }

            if (fromDate && toDate) {
                const [fromYear, fromMonth] = fromDate.split("-").map(Number);
                const [toYear, toMonth] = toDate.split("-").map(Number);

                const startDate = new Date(fromYear, fromMonth - 1, 1);
                const endDate = new Date(toYear, toMonth, 0);

                filter.date = {
                    $gte: startDate.toISOString().split("T")[0],
                    $lte: endDate.toISOString().split("T")[0]
                };
            }

            if (artist === "true") filter.track_artist = { $nin: ["", null, undefined] };
            if (territory === "true") filter.territory = { $nin: ["", null, undefined] };
            if (releases === "true") filter.release = { $nin: ["", null, undefined] };

            const pipeline = [
                { $match: filter },
                { $sort: { date: -1 } },
                { $project: { __v: 0, createdAt: 0, updatedAt: 0 } }
            ];

            console.log(`Filter for report ${reportId}:`, JSON.stringify(filter, null, 2));

            const count = await TblReport2025.countDocuments(filter);
            console.log(`Total records found for report ${reportId}: ${count}`);

            if (count === 0) {
                await YoutubeReportHistory.findByIdAndUpdate(reportId, {
                    status: 'failed',
                    error: 'No data found',
                });
                return;
            }

            // NEW: Create file path early
            const timestamp = new Date().toISOString().split('T')[0].replace(/-/g, '');
            const randomSuffix = Math.random().toString(36).substring(2, 8);
            const filename = `Revenue_Report_${timestamp}_${randomSuffix}.csv`;

            const relativeFolder = 'reports';
            const absoluteFolder = path.join(__dirname, '../uploads', relativeFolder);

            if (!fs.existsSync(absoluteFolder)) {
                fs.mkdirSync(absoluteFolder, { recursive: true });
            }

            const absoluteFilePath = path.join(absoluteFolder, filename);
            const relativePath = `uploads/reports/${filename}`;
            const fileURL = `${process.env.BASE_URL}/${relativePath}`;

            // NEW: Streaming approach – no more huge "data" array!
            const writeStream = fs.createWriteStream(absoluteFilePath);

            // We'll write headers after getting the first document (dynamic headers)
            let headersWritten = false;
            let headers = ["S.No"];
            let rowIndex = 1;

            const excludeFields = ["_id", "__v", "createdAt", "updatedAt"];

            // Use aggregation cursor for true streaming (low memory)
            const collection = mongoose.connection.db.collection('tblreport_2025');
            const cursor = collection.aggregate(pipeline, {
                allowDiskUse: true,
                cursor: { batchSize: 1000 } // Adjust batch size if needed
            });

            // Create a fast-csv formatter that writes directly to file
            const csvStream = fastCsv.format({ headers: false, includeEndRowDelimiter: true });

            csvStream.pipe(writeStream);

            let firstDoc = true;

            for await (const doc of cursor) {
                if (firstDoc) {
                    // Build headers from first document (same as your original code)
                    Object.keys(doc).forEach(key => {
                        if (!excludeFields.includes(key) && key !== "date") {
                            headers.push(key);
                        }
                    });
                    headers.push("date");

                    // Write header row
                    csvStream.write(headers);
                    headersWritten = true;
                    firstDoc = false;
                }

                // Build row data
                const rowData = [rowIndex++];

                Object.keys(doc).forEach(key => {
                    if (!excludeFields.includes(key) && key !== "date") {
                        rowData.push(doc[key] ?? "");
                    }
                });
                rowData.push(doc.date ?? "");

                csvStream.write(rowData);

                // Optional: log progress
                if (rowIndex % 10000 === 0) {
                    console.log(`Streamed ${rowIndex} rows...`);
                }
            }

            // End the streams
            csvStream.end();
            cursor.close();

            // Wait for file to finish writing
            await new Promise((resolve, reject) => {
                writeStream.on('finish', resolve);
                writeStream.on('error', reject);
            });

            console.log(`CSV report saved: ${absoluteFilePath} (${rowIndex - 1} rows)`);

            // Update history
            await YoutubeReportHistory.findByIdAndUpdate(reportId, {
                status: 'ready',
                filename,
                filePath: relativePath,
                fileURL,
            });

            console.log(`Report ${reportId} successfully generated as CSV`);

        } catch (error) {
            console.error(`Error processing report ${reportId}:`, error);
            await YoutubeReportHistory.findByIdAndUpdate(reportId, {
                status: 'failed',
                error: error.message || 'Unknown error',
            });
            throw error;
        }
    }

    // Process pending audio streaming reports
    async processPendingReports() {
        try {
            const existingGenerate = await AudioStreamingReportHistory.exists({
                status: { $in: ['generate', 'generating'] }
            });

            if (existingGenerate) {
                console.log('Generate process already in progress. Skipping pending updates.');
                return;
            }

            const updatedReports = await AudioStreamingReportHistory.updateMany(
                { status: 'pending' },
                {
                    $set: {
                        status: 'generate',
                        filename: 'Preparing to generate...'
                    }
                }
            );

            console.log(`Updated ${updatedReports.modifiedCount} reports from pending to generate`);

            const generateReports = await AudioStreamingReportHistory.find({
                status: 'generate'
            }).sort({ generatedAt: 1 });

            console.log(`Found ${generateReports.length} pending Audio reports to process`);

            for (const report of generateReports) {
                const THIRTY_MINUTES = 30 * 60 * 1000;
                const reportAge =
                    Date.now() - new Date(report.generatedAt).getTime();

                if (reportAge > THIRTY_MINUTES) {
                    await AudioStreamingReportHistory.findByIdAndUpdate(report._id, {
                        status: 'failed',
                        error: 'Processing timeout'
                    });
                    continue;
                }

                const lockedReport = await AudioStreamingReportHistory.findOneAndUpdate(
                    { _id: report._id, status: 'generate' },
                    {
                        $set: {
                            status: 'generating',
                            filename: 'Generating report...',
                            processingStartedAt: new Date()
                        }
                    },
                    { new: true }
                );

                if (!lockedReport) continue;

                await this.processAudioStreamingReport(report._id, report.filters);
            }
        } catch (error) {
            console.error('Error in processPendingReports cron job:', error);
        }
    }

    // Process pending YouTube reports
    async processPendingYoutubeReports() {
        try {
            const existingGenerate = await YoutubeReportHistory.exists({
                status: { $in: ['generate', 'generating'] }
            });

            if (existingGenerate) {
                console.log('Generate process already in progress. Skipping pending updates.');
                return;
            }

            const updatedReports = await YoutubeReportHistory.updateMany(
                { status: 'pending' },
                {
                    $set: {
                        status: 'generate',
                        filename: 'Preparing to generate...'
                    }
                }
            );

            console.log(`Updated ${updatedReports.modifiedCount} youtube reports from pending to generate`);

            const generateReports = await YoutubeReportHistory.find({
                status: 'generate'
            }).sort({ generatedAt: 1 });

            console.log(`Found ${generateReports.length} pending YouTube reports to process`);

            for (const report of generateReports) {
                const THIRTY_MINUTES = 30 * 60 * 1000;
                const reportAge = Date.now() - new Date(report.generatedAt).getTime();

                if (reportAge > THIRTY_MINUTES) {
                    await YoutubeReportHistory.findByIdAndUpdate(report._id, {
                        status: 'failed',
                        error: 'Processing timeout'
                    });
                    continue;
                }

                const lockedReport = await YoutubeReportHistory.findOneAndUpdate(
                    { _id: report._id, status: 'generate' },
                    {
                        $set: {
                            status: 'generating',
                            filename: 'Generating report...',
                            processingStartedAt: new Date()
                        }
                    },
                    { new: true }
                );

                if (!lockedReport) continue;

                await this.processYoutubeReport(report._id, report.filters);
            }
        } catch (error) {
            console.error('Error in processPendingYoutubeReports cron job:', error);
        }
    }

    // Combined cron job to process all pending reports
    async processAllPendingReports() {
        try {
            console.log('Processing all pending reports...');

            // Process audio streaming reports
            await this.processPendingReports();

            // Process YouTube reports
            await this.processPendingYoutubeReports();

            console.log('All pending reports processed');
        } catch (error) {
            console.error("Error in processAllPendingReports:", error);
        }
    }

    // deleteRevenueByUserId method
    async deleteRevenueByUserId(req, res, next) {
        try {
            const { userId } = req.query;

            if (!userId) {
                return res.status(400).json({
                    success: false,
                    message: "User ID is required"
                });
            }

            // Delete upload entry
            const revenueUploadResult = await RevenueUpload.findByIdAndDelete(userId);

            if (!revenueUploadResult) {
                return res.status(404).json({
                    success: false,
                    message: "RevenueUpload record not found"
                });
            }

            // Delete revenue reports
            const tempReportResult = await TempReport.deleteMany({
                uploadId: userId
            });

            return res.status(200).json({
                success: true,
                message: "Revenue data deleted successfully",
                tempReportDeletedCount: tempReportResult.deletedCount,
                revenueUploadDeleted: true
            });

        } catch (error) {
            console.error("Error deleting revenue by user ID:", error);
            return res.status(500).json({
                success: false,
                message: "Internal server error"
            });
        }
    }

    //getReportHistory method
    async getReportHistory(req, res, next) {
        try {
            const { role, userId } = req.user;

            const query = {};

            if (role !== "Super Admin" && role !== "Manager") {
                query.user_id = userId;
            }
            const reports = await AudioStreamingReportHistory.find(query)
                .select('filename filePath fileURL status generatedAt user_id')
                .sort({ generatedAt: -1 })
                .lean();

            return res.status(200).json({
                success: true,
                count: reports.length,
                data: reports
            });

        } catch (error) {
            console.error("Error fetching report history:", error);

            return res.status(500).json({
                success: false,
                message: "Failed to fetch report history",
                error: error.message
            });
        }
    }

    //deleteReportHistory method
    async deleteReportHistory(req, res, next) {
        try {
            const { id } = req.query;

            if (!id) {
                return res.status(400).json({
                    success: false,
                    message: "Report ID is required"
                });
            }

            const report = await AudioStreamingReportHistory.findById(id);

            if (!report) {
                return res.status(404).json({
                    success: false,
                    message: "Report not found"
                });
            }

            let fileDeleted = false;

            if (report.filePath) {
                // 🔑 FORCE src BASE
                const absolutePath = path.resolve(
                    process.cwd(),
                    'src',
                    report.filePath.replace(/^src[\\/]/, '')
                );

                console.log("Deleting file:", absolutePath);

                if (fs.existsSync(absolutePath)) {
                    fs.unlinkSync(absolutePath);
                    fileDeleted = true;
                }
            }

            await AudioStreamingReportHistory.findByIdAndDelete(id);

            return res.json({
                success: true,
                message: "Report and file deleted successfully",
                fileDeleted
            });

        } catch (error) {
            console.error("Delete error:", error);
            next(error);
        }
    }

    //getYoutubeReportHistory method
    async getYoutubeReportHistory(req, res, next) {
        try {
            const { role, userId } = req.user;

            const query = {};

            if (role !== "Super Admin" && role !== "Manager") {
                query.user_id = userId;
            }
            const reports = await YoutubeReportHistory.find(query)
                .select('filename filePath fileURL status generatedAt')
                .sort({ generatedAt: -1 })
                .lean();

            return res.status(200).json({
                success: true,
                count: reports.length,
                data: reports
            });

        } catch (error) {
            console.error("Error fetching report history:", error);

            return res.status(500).json({
                success: false,
                message: "Failed to fetch report history",
                error: error.message
            });
        }
    }

    //deleteYoutubeReportHistory method
    async deleteYoutubeReportHistory(req, res, next) {
        try {
            const { id } = req.query;

            if (!id) {
                return res.status(400).json({
                    success: false,
                    message: "Report ID is required"
                });
            }

            const report = await YoutubeReportHistory.findById(id);

            if (!report) {
                return res.status(404).json({
                    success: false,
                    message: "Report not found"
                });
            }

            let fileDeleted = false;

            if (report.filePath) {
                // 🔑 FORCE src BASE
                const absolutePath = path.resolve(
                    process.cwd(),
                    'src',
                    report.filePath.replace(/^src[\\/]/, '')
                );

                console.log("Deleting file:", absolutePath);

                if (fs.existsSync(absolutePath)) {
                    fs.unlinkSync(absolutePath);
                    fileDeleted = true;
                }
            }

            await YoutubeReportHistory.findByIdAndDelete(id);

            return res.json({
                success: true,
                message: "Report and file deleted successfully",
                fileDeleted
            });

        } catch (error) {
            console.error("Delete error:", error);
            next(error);
        }
    }

    async insertBatch(batch) {
        if (!batch || batch.length === 0) return 0;

        try {
            const result = await TblReport2025.insertMany(batch, {
                ordered: false,
                bypassDocumentValidation: true
            });

            const inserted = result.length;
            batch.length = 0; // 🔥 FREE MEMORY
            return inserted;

        } catch (error) {
            // Partial success handling
            if (error.insertedDocs) {
                const inserted = error.insertedDocs.length;
                batch.length = 0;
                return inserted;
            }

            console.error('Batch insert error:', error);
            batch.length = 0;
            return 0;
        }
    }

    async importRevenueFromJson(req, res) {
        try {
            const { filePath, uploadId, userId } = req.body;

            if (!filePath || !uploadId) {
                return res.status(400).json({
                    success: false,
                    message: 'filePath and uploadId are required'
                });
            }

            const absolutePath = path.resolve(filePath);
            console.log('absolutePath:', absolutePath);

            if (!fs.existsSync(absolutePath)) {
                return res.status(404).json({
                    success: false,
                    message: 'JSON file not found'
                });
            }

            // 🔄 Streaming pipeline (NO readFileSync)
            const pipeline = chain([
                fs.createReadStream(absolutePath, { highWaterMark: 1024 * 1024 }),
                parser(),
                streamArray()
            ]);

            let batch = [];
            let totalInserted = 0;

            for await (const { value } of pipeline) {
                const formattedDate = getDateFromMonthYear(
                    value.month,
                    value.year
                );

                batch.push({
                    user_id: value.label_id || 0,
                    uploadId,

                    retailer: value.channel_name,
                    // retailer: 'Facebook',
                    label: value.label_name || null,

                    upc_code: null,
                    catalogue_number: null,

                    isrc_code: value.isrc || value.elected_isrc || null,

                    // release: value.track_name || null,
                    // track_title: value.track_name || null,
                    // track_artist: value.artist_name || null,
                    release: value.asset_title || null,
                    track_title: value.asset_title || null,
                    track_artist: value.label_name || null,

                    remixer_name: null,
                    remix: null,

                    territory: value.country === 'N/A' ? null : value.country,

                    purchase_status: null,
                    format: value.product || null,
                    delivery: 'Streaming',

                    content_type: null,
                    track_count: value.total_play || null,
                    sale_type: null,

                    net_total: value.total_revenue || null,

                    date: formattedDate,
                    uploading_date: formattedDate
                });

                // 💾 Batch insert
                if (batch.length === BATCH_SIZE) {
                    totalInserted += await this.insertBatch(batch);
                }
            }

            // 🧹 Insert remaining
            totalInserted += await this.insertBatch(batch);
            console.log("totalInserted", totalInserted);


            return res.status(200).json({
                success: true,
                message: 'Revenue data imported successfully',
                totalInserted
            });

        } catch (error) {
            console.error('Import JSON Error:', error);
            return res.status(500).json({
                success: false,
                message: 'Internal server error'
            });
        }
    }


}

module.exports = new revenueUploadController();