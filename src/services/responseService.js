class ResponseService {
    static success(res, message = "Success", data = {}) {
        return res.status(200).json({
            success: true,
            message,
            ...data,
        });
    }

    static created(res, message = "Resource created successfully", data = {}) {
        return res.status(201).json({
            success: true,
            message,
            ...data,
        });
    }

    static error(res, message = "Something went wrong", status = 500, error = {}) {
        return res.status(status).json({
            success: false,
            message,
            error: error.message || error,
        });
    }
}

module.exports = ResponseService;
