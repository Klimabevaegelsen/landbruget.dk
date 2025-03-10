# fs.tf | File System Configuration for Mage AI

# Create a Filestore instance for persistent storage
# Commented out in favor of cheaper NFS solution
# resource "google_filestore_instance" "instance" {
#   name     = "${var.app_name}"
#   location = var.zone
#   tier     = "BASIC_HDD"
#
#   file_shares {
#     capacity_gb = 1024
#     name        = "share1"
#   }
#
#   networks {
#     network = "default"
#     modes   = ["MODE_IPV4"]
#   }
# }

# Create a VPC Access Connector for private connectivity
resource "google_vpc_access_connector" "connector" {
  name           = "${var.app_name}-connector"
  ip_cidr_range  = "10.8.0.0/28"
  region         = var.region
  network        = "default"
  machine_type   = "e2-micro"
  min_instances  = 2  # Google Cloud requires at least 2 instances
  max_instances  = 3  # Must be greater than min_instances
  min_throughput = 200
  max_throughput = 300
}

# ---------------------------------------------------
# Use a cheaper NFS provisioned on GCP Compute Engine.
# 1. Uncomment the module "nfs" below
# 2. Update the environment variable in google_cloud_run_service.run_service.
#    Set "FILESTORE_IP_ADDRESS" to module.nfs.internal_ip and
#    "FILE_SHARE_NAME" to "share/mage".
# 3. Comment out the google_filestore_instance.instance resource.
# ---------------------------------------------------

module "nfs" {
  source  = "DeimosCloud/nfs/google"
  version = "1.0.1"
  name_prefix = "${var.app_name}-nfs"
  # labels      = local.common_labels
  # subnetwork  = module.vpc.public_subnetwork
  attach_public_ip = true
  project       = var.project_id
  network       = "default"
  machine_type  = "e2-micro"
  source_image_project  = "debian-cloud"
  image_family  = "debian-11-bullseye-v20230629"
  export_paths  = [
    "/share/mage",
  ]
  capacity_gb = "50"
}
