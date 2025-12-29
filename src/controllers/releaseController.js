const Release = require("../models/releaseModel");
const ResponseService = require("../services/responseService");

class ReleaseController {

    constructor() { }

    // getAllReleases method
    async getAllReleases(req, res, next) {
        try {
            const { role, userId } = req.user;

            const { page = 1, limit = 20, search } = req.query;
            const skip = (page - 1) * limit;
            const limitNum = Number(limit);

            let query = {};

            if (role !== "Super Admin" && role !== "Manager") {
                query.label_id = userId;
            }

            // Search filtering
            if (search && search.trim() !== "") {
                const regex = new RegExp(search.trim(), "i");
                query.$or = [
                    { title: regex },
                    { artists: regex },
                    { upc_number: regex },
                    { display_artist: regex },
                    { feature_artist: regex }
                ];
            }

            const aggregation = [
                { $match: query },
                { $sort: { createdAt: -1 } },
                { $skip: skip },
                { $limit: limitNum },
                {
                    $lookup: {
                        from: "tracks",
                        let: { releaseIsrc: "$isrc" },
                        pipeline: [
                            {
                                $match: {
                                    $expr: {
                                        $and: [
                                            {
                                                $eq: [
                                                    { $trim: { input: { $toUpper: "$isrc_number" } } },
                                                    { $trim: { input: { $toUpper: "$$releaseIsrc" } } }
                                                ]
                                            },
                                            { $ne: ["$$releaseIsrc", null] },
                                            { $ne: ["$$releaseIsrc", ""] },
                                            { $ne: ["$isrc_number", null] },
                                            { $ne: ["$isrc_number", ""] }
                                        ]
                                    }
                                }
                            }
                        ],
                        as: "tracks"
                    }
                },
                {
                    $addFields: {
                        total_tracks: { $size: "$tracks" }
                    }
                },
                { $unset: "tracks" }
            ];

            const releases = await Release.aggregate(aggregation);
            const totalResult = await Release.aggregate([
                { $match: query },
                { $count: "total" }
            ]);

            const total = totalResult.length > 0 ? totalResult[0].total : 0;

            return ResponseService.success(res, "Releases fetched successfully", {
                releases,
                pagination: {
                    total,
                    page: Number(page),
                    limit: limitNum,
                    totalPages: Math.ceil(total / limitNum),
                },
            });
        } catch (error) {
            console.error("Error in getAllReleases:", error);
            return ResponseService.error(res, "Failed to fetch releases", 500, error);
        }
    }


}

module.exports = new ReleaseController();
