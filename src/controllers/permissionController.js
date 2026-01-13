const RolePermission = require('../models/rolePermissionModel');

class PermissionController {
    constructor() {

    }


    async getAllPermissions(req, res) {
        try {
            const permissions = await RolePermission.find();
            return res.status(200).json(permissions);
        } catch (err) {
            console.error('Error fetching all permissions:', err);
            return res.status(500).json({ message: 'Server error', error: err.message });
        }
    }

    async getPermissionsByRole(req, res) {
        try {
            const { role } = req.params;

            const permission = await RolePermission.findOne({ role });

            if (!permission) {
                return res.status(404).json({ message: `Permissions not found for role: ${role}` });
            }

            return res.status(200).json(permission.allowedKeys);
        } catch (err) {
            console.error(`Error fetching permissions for role ${req.params.role}:`, err);
            return res.status(500).json({ message: 'Server error', error: err.message });
        }
    }

    async updatePermissions(req, res) {
        try {
            const { role } = req.params;
            const { allowedKeys } = req.body;

            if (!Array.isArray(allowedKeys)) {
                return res.status(400).json({ message: 'allowedKeys must be an array' });
            }

            const updated = await RolePermission.findOneAndUpdate(
                { role },
                {
                    allowedKeys,
                    updatedAt: new Date()
                },
                {
                    new: true,
                    upsert: true,
                    runValidators: true
                }
            );

            return res.status(200).json({
                message: 'Permissions updated successfully',
                data: updated
            });
        } catch (err) {
            console.error(`Error updating permissions for role ${req.params.role}:`, err);
            return res.status(500).json({ message: 'Server error', error: err.message });
        }
    }
}

// Export as singleton instance
module.exports = new PermissionController();