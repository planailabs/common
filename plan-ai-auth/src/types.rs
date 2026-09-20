use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Organization membership with role information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrgMembership {
    pub org_id: Uuid,
    pub role: String, // "admin", "write", "read"
    /// Permissions granted on top of the role, for things that are not a
    /// point on the read/write scale — paying the bill, buying a service.
    /// An organization admin has all of them without being listed here.
    ///
    /// Defaulted so a session stored before permissions existed still loads.
    #[serde(default)]
    pub permissions: Vec<String>,
}

impl OrgMembership {
    /// A membership with no permissions beyond its role.
    pub fn new(org_id: Uuid, role: impl Into<String>) -> Self {
        Self {
            org_id,
            role: role.into(),
            permissions: Vec::new(),
        }
    }
}

/// Lightweight user context extracted from the OIDC session and stored
/// in axum request extensions for use in Dioxus server functions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebUser {
    pub id: Uuid,
    pub email: String,
    pub name: String,
    pub is_admin: bool,
    pub org_memberships: Vec<OrgMembership>,
    /// If set, this user context is the result of admin impersonation.
    /// The value is the real admin's user ID.
    #[serde(default)]
    pub impersonating_from: Option<Uuid>,
}

impl WebUser {
    /// All organization IDs this user belongs to (any role).
    pub fn org_ids(&self) -> Vec<Uuid> {
        self.org_memberships.iter().map(|m| m.org_id).collect()
    }

    /// Organization IDs where user has write or admin role.
    pub fn write_org_ids(&self) -> Vec<Uuid> {
        self.org_memberships
            .iter()
            .filter(|m| m.role == "admin" || m.role == "write")
            .map(|m| m.org_id)
            .collect()
    }

    /// Whether the user holds a named permission in an organization.
    ///
    /// Admins hold everything: an organization's admin can already grant
    /// themselves the permission, so withholding it would only be theatre.
    pub fn has_org_permission(&self, org_id: &Uuid, permission: &str) -> bool {
        self.is_org_admin(org_id)
            || self.org_memberships.iter().any(|m| {
                m.org_id == *org_id && m.permissions.iter().any(|p| p == permission)
            })
    }

    /// Organizations where the user holds a permission.
    pub fn permitted_org_ids(&self, permission: &str) -> Vec<Uuid> {
        self.org_memberships
            .iter()
            .filter(|m| {
                m.role == "admin" || m.permissions.iter().any(|p| p == permission)
            })
            .map(|m| m.org_id)
            .collect()
    }

    /// Check if user is an admin of a specific organization.
    pub fn is_org_admin(&self, org_id: &Uuid) -> bool {
        self.is_admin
            || self
                .org_memberships
                .iter()
                .any(|m| m.org_id == *org_id && m.role == "admin")
    }

    /// Require global admin access. Returns a string error suitable for
    /// conversion into framework-specific error types.
    pub fn require_admin_str(&self) -> Result<(), &'static str> {
        if self.is_admin {
            Ok(())
        } else {
            Err("admin access required")
        }
    }

    /// Require org admin access. Returns a string error.
    pub fn require_org_admin_str(&self, org_id: &Uuid) -> Result<(), &'static str> {
        if self.is_org_admin(org_id) {
            Ok(())
        } else {
            Err("organization admin access required")
        }
    }
}
