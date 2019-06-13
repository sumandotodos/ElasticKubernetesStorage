dokubectl delete pods/k8s-elastic-storage-controller
dokubectl delete sts/storagecells-sts
dokubectl delete services/k8s-elastic-storage-service
dokubectl delete services/storage-cells-service
dokubectl delete pvc/mongodb-pv-claim
dokubectl delete pvc/cellvolume-storagecells-sts-2
dokubectl delete pvc/cellvolume-storagecells-sts-1
dokubectl delete pvc/cellvolume-storagecells-sts-0

