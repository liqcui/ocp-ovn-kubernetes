package ops

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	libovsdbclient "github.com/ovn-kubernetes/libovsdb/client"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/sbdb"
)

// ListChassis looks up all chassis from the cache
func ListChassis(sbClient libovsdbclient.Client) ([]*sbdb.Chassis, error) {
	ctx, cancel := context.WithTimeout(context.Background(), config.Default.OVSDBTxnTimeout)
	defer cancel()
	searchedChassis := []*sbdb.Chassis{}
	err := sbClient.List(ctx, &searchedChassis)
	return searchedChassis, err
}

// ListChassisPrivate looks up all chassis private models from the cache
func ListChassisPrivate(sbClient libovsdbclient.Client) ([]*sbdb.ChassisPrivate, error) {
	ctx, cancel := context.WithTimeout(context.Background(), config.Default.OVSDBTxnTimeout)
	defer cancel()
	found := []*sbdb.ChassisPrivate{}
	err := sbClient.List(ctx, &found)
	return found, err
}

// GetChassis looks up a chassis from the cache using the 'Name' column which is an indexed
// column.
func GetChassis(sbClient libovsdbclient.Client, chassis *sbdb.Chassis) (*sbdb.Chassis, error) {
	found := []*sbdb.Chassis{}
	opModel := operationModel{
		Model:          chassis,
		ExistingResult: &found,
		ErrNotFound:    true,
		BulkOp:         false,
	}

	m := newModelClient(sbClient)
	err := m.Lookup(opModel)
	if err != nil {
		return nil, err
	}

	return found[0], nil
}

// DeleteChassis deletes the provided chassis and associated private chassis
func DeleteChassis(sbClient libovsdbclient.Client, chassis ...*sbdb.Chassis) error {
	opModels := make([]operationModel, 0, len(chassis))
	for i := range chassis {
		foundChassis := []*sbdb.Chassis{}
		chassisPrivate := sbdb.ChassisPrivate{
			Name: chassis[i].Name,
		}
		chassisUUID := ""
		opModel := []operationModel{
			{
				Model:          chassis[i],
				ExistingResult: &foundChassis,
				ErrNotFound:    false,
				BulkOp:         false,
				DoAfter: func() {
					if len(foundChassis) > 0 {
						chassisPrivate.Name = foundChassis[0].Name
						chassisUUID = foundChassis[0].UUID
					}
				},
			},
			{
				Model:       &chassisPrivate,
				ErrNotFound: false,
				BulkOp:      false,
			},
			// IGMPGroup has a weak link to chassis, deleting multiple chassis may result in IGMP_Groups
			// with identical values on columns "address", "datapath", and "chassis", when "chassis" goes empty
			{
				Model: &sbdb.IGMPGroup{},
				ModelPredicate: func(group *sbdb.IGMPGroup) bool {
					return group.Chassis != nil && chassisUUID != "" && *group.Chassis == chassisUUID
				},
				ErrNotFound: false,
				BulkOp:      true,
			},
		}
		opModels = append(opModels, opModel...)
	}

	// Deduplicate IGMP_Group deletions to prevent constraint violations
	opModels = deduplicateIGMPGroupDeletions(opModels)

	m := newModelClient(sbClient)
	err := m.Delete(opModels...)
	return err
}

type chassisPredicate func(*sbdb.Chassis) bool

// deduplicateIGMPGroupDeletions ensures that when deleting chassis, we don't
// create duplicate IGMP_Group entries with identical (address, datapath, chassis) values.
// When multiple chassis are deleted, their IGMP_Group entries may have the same
// address and datapath but different chassis references. When chassis is set to nil,
// this violates the database unique constraint on (address, datapath, chassis).
//
// This function deduplicates IGMP_Group deletion operations to prevent constraint
// violations that cause transaction failures and forced recomputes.
//
// Root cause: IPv6 solicited-node multicast addresses (ff02::1:ff*) are shared
// by pods with different interfaces on the same chassis and datapath.
func deduplicateIGMPGroupDeletions(opModels []operationModel) []operationModel {
	seen := make(map[string]bool)
	filtered := make([]operationModel, 0, len(opModels))
	duplicateCount := 0

	for _, opModel := range opModels {
		// Only deduplicate IGMP_Group delete operations
		group, isIGMPGroup := opModel.Model.(*sbdb.IGMPGroup)
		if !isIGMPGroup || opModel.ModelPredicate == nil {
			// Keep all non-IGMP operations and IGMP operations without predicates
			filtered = append(filtered, opModel)
			continue
		}

		// Create unique key matching the database unique index: address|datapath|chassis
		// Note: We use empty string for nil chassis since after deletion chassis will be nil
		chassisKey := ""
		if group.Chassis != nil {
			chassisKey = *group.Chassis
		}
		datapathKey := ""
		if group.Datapath != nil {
			datapathKey = *group.Datapath
		}
		key := fmt.Sprintf("%s|%s|%s", group.Address, datapathKey, chassisKey)

		if !seen[key] {
			filtered = append(filtered, opModel)
			seen[key] = true
			klog.V(5).Infof("Including IGMP_Group deletion for key: %s", key)
		} else {
			duplicateCount++
			klog.V(5).Infof("Skipping duplicate IGMP_Group deletion for key: %s", key)
		}
	}

	if duplicateCount > 0 {
		klog.V(4).Infof("Deduplicated %d IGMP_Group deletion operations to prevent constraint violations", duplicateCount)
	}

	return filtered
}

// DeleteChassisWithPredicate looks up chassis from the cache based on a given
// predicate and deletes them as well as the associated private chassis
func DeleteChassisWithPredicate(sbClient libovsdbclient.Client, p chassisPredicate) error {
	foundChassis := []*sbdb.Chassis{}
	foundChassisNames := sets.NewString()
	foundChassisUUIDS := sets.NewString()
	opModels := []operationModel{
		{
			Model:          &sbdb.Chassis{},
			ModelPredicate: p,
			ExistingResult: &foundChassis,
			ErrNotFound:    false,
			BulkOp:         true,
			DoAfter: func() {
				for _, chassis := range foundChassis {
					foundChassisNames.Insert(chassis.Name)
					foundChassisUUIDS.Insert(chassis.UUID)
				}
			},
		},
		{
			Model:          &sbdb.ChassisPrivate{},
			ModelPredicate: func(item *sbdb.ChassisPrivate) bool { return foundChassisNames.Has(item.Name) },
			ErrNotFound:    false,
			BulkOp:         true,
		},
		// IGMPGroup has a weak link to chassis, deleting multiple chassis may result in IGMP_Groups
		// with identical values on columns "address", "datapath", and "chassis", when "chassis" goes empty
		{
			Model:          &sbdb.IGMPGroup{},
			ModelPredicate: func(group *sbdb.IGMPGroup) bool { return group.Chassis != nil && foundChassisUUIDS.Has(*group.Chassis) },
			ErrNotFound:    false,
			BulkOp:         true,
		},
	}

	// Deduplicate IGMP_Group deletions to prevent constraint violations
	opModels = deduplicateIGMPGroupDeletions(opModels)

	m := newModelClient(sbClient)
	err := m.Delete(opModels...)
	return err
}

// CreateOrUpdateChassis creates or updates the chassis record along with the encap record
func CreateOrUpdateChassis(sbClient libovsdbclient.Client, chassis *sbdb.Chassis, encaps ...*sbdb.Encap) error {
	m := newModelClient(sbClient)
	opModels := make([]operationModel, 0, len(encaps)+1)
	for i := range encaps {
		encap := encaps[i]
		opModel := operationModel{
			Model: encap,
			DoAfter: func() {
				encapsList := append(chassis.Encaps, encap.UUID)
				chassis.Encaps = sets.New(encapsList...).UnsortedList()
			},
			OnModelUpdates: onModelUpdatesNone(),
			ErrNotFound:    false,
			BulkOp:         false,
		}
		opModels = append(opModels, opModel)
	}

	opModel := operationModel{
		Model:            chassis,
		OnModelMutations: []interface{}{&chassis.OtherConfig},
		OnModelUpdates:   []interface{}{&chassis.Encaps},
		ErrNotFound:      false,
		BulkOp:           false,
	}

	opModels = append(opModels, opModel)
	if _, err := m.CreateOrUpdate(opModels...); err != nil {
		return err
	}

	return nil
}

// validateRequestedChassisOption is a guard to ensure a caller is using the chassis-id (uuid format)
// for the requested chassis option.
func validateRequestedChassisOption(options map[string]string) error {
	if len(options) == 0 {
		return nil
	}
	chassisID, ok := options[RequestedChassis]
	if !ok || chassisID == "" {
		return nil
	}
	if _, err := uuid.Parse(chassisID); err != nil {
		return fmt.Errorf("requested-chassis must be a valid UUID, got %q", chassisID)
	}
	return nil
}
