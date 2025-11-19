const Artist = require("../models/artistModel");
const ResponseService = require("../services/responseService");
const User = require("../models/userModel");

class ArtistController {

    constructor() { }

    //getAllArtists method
    async getAllArtists(req, res, next) {
        try {
            console.log("req.user", req.user)
            const { role, userId } = req.user;
            console.log("userId", userId);
            console.log("login")
            const { page = 1, limit = 20, search } = req.query;

            const skip = (page - 1) * limit;

            // ---- BASE QUERY ----
            let query = {};

            // ✅ If NOT Super Admin → filter by userId
            if (role !== "Super Admin") {
                query.created_by = userId;
                console.log("labelll");
            }

            // ✅ Search filter
            if (search && search.trim() !== "") {
                const regex = new RegExp(search, "i");
                query.name = regex;
            }

            // ---- FETCH DATA ----
            const artists = await Artist.find(query)
                .skip(skip)
                .limit(Number(limit));

            const total = await Artist.countDocuments(query);

            return ResponseService.success(res, "Artists fetched successfully....", {
                artists,
                pagination: {
                    total,
                    page: Number(page),
                    limit: Number(limit),
                    totalPages: Math.ceil(total / limit)
                }
            });

        } catch (error) {
            return ResponseService.error(res, "Failed to fetch artists", 500, error);
        }
    }


    //fetchArtistById method
    async fetchArtistById(req, res) {
        try {
            const { id } = req.query;

            if (!id) {
                return ResponseService.error(res, "id is required", 400);
            }

            const artist = await Artist.find({ id: id });

            if (!artist) {
                return ResponseService.error(res, "Artist not found", 404);
            }

            return ResponseService.success(res, "Artist details fetched successfully", { artist });

        } catch (error) {
            return ResponseService.error(res, "Failed to fetch artist", 500, error);
        }
    }

    //fetchUserAndSubUsersArtist method
    async fetchUserAndSubUsersArtist(req, res) {
        try {
            const { id } = req.query;

            if (!id) {
                return ResponseService.error(res, "id is required", 400);
            }

            const numericId = Number(id);

            // Find main user + sub users
            const users = await User.find({
                $or: [
                    { id: numericId },
                    { parent_id: numericId }
                ]
            });

            // Extract all user ids
            const userIds = users.map(u => u.id);

            // Fetch all artists created by these users
            const artists = await Artist.find({
                created_by: { $in: userIds }
            });

            return ResponseService.success(res, "Artist details fetched successfully", { artists });

        } catch (error) {
            return res.status(500).json({ success: false, message: error.message });
        }
    }


}

module.exports = new ArtistController();
