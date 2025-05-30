package stream

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	svcsdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
)

func (rm *resourceManager) updateRetentionPeriodHours(
	ctx context.Context,
	latest *resource,
	desired *resource,
) (err error) {
	desiredRetentionPeriod := desired.ko.Spec.RetentionPeriodHours
	latestRetentionPeriod := latest.ko.Spec.RetentionPeriodHours
	streamName := latest.ko.Spec.Name

	if desiredRetentionPeriod == nil || latestRetentionPeriod == nil {
		return nil
	}
	desiredRetentionPeriodInt32 := int32(*desiredRetentionPeriod)
	latestRetentionPeriodInt32 := int32(*latestRetentionPeriod)

	if desiredRetentionPeriodInt32 < 24 || latestRetentionPeriodInt32 > 8760 {
		return errors.New("the desired retention period must be between 24 and 8760 hours")
	}

	switch {
	case *desiredRetentionPeriod > *latestRetentionPeriod:
		input := &svcsdk.IncreaseStreamRetentionPeriodInput{
			StreamName:           streamName,
			RetentionPeriodHours: &desiredRetentionPeriodInt32,
		}
		_, err := rm.sdkapi.IncreaseStreamRetentionPeriod(ctx, input)
		rm.metrics.RecordAPICall("UPDATE", "IncreaseStreamRetentionPeriod", err)
		return err
	case *desiredRetentionPeriod < *latestRetentionPeriod:
		input := &svcsdk.DecreaseStreamRetentionPeriodInput{
			StreamName:           streamName,
			RetentionPeriodHours: &latestRetentionPeriodInt32,
		}
		_, err := rm.sdkapi.DecreaseStreamRetentionPeriod(ctx, input)
		rm.metrics.RecordAPICall("UPDATE", "DecreaseStreamRetentionPeriod", err)
		return err
	}
	return nil
}

func isEncryptionDisabled(encryptionType *string) bool {
	return encryptionType == nil || *encryptionType == "" || *encryptionType == string(types.EncryptionTypeNone)
}

func isEncryptionEnabled(encryptionType *string) bool {
	return encryptionType != nil && *encryptionType == string(types.EncryptionTypeKms)
}

func (rm *resourceManager) updateStreamEncryption(
	ctx context.Context,
	latest *resource,
	desired *resource,
) (err error) {
	desiredEncryptionType := desired.ko.Spec.EncryptionType
	latestEncryptionType := latest.ko.Spec.EncryptionType

	if isEncryptionEnabled(desiredEncryptionType) && (desired.ko.Spec.KeyID == nil || *desired.ko.Spec.KeyID == "") {
		return errors.New("KMS encryption type requires a KeyID")
	}
	if isEncryptionDisabled(desiredEncryptionType) && desired.ko.Spec.KeyID != nil && *desired.ko.Spec.KeyID != "" {
		return errors.New("cannot specify KeyID with NONE encryption type")
	}

	// if latestEncryptionType is kms and desiredEncryptionType is disabled, we need to stop the encryption
	if isEncryptionEnabled(latestEncryptionType) {
		if isEncryptionDisabled(desiredEncryptionType) {
			input := &svcsdk.StopStreamEncryptionInput{
				StreamName:     latest.ko.Spec.Name,
				EncryptionType: types.EncryptionTypeKms,
				KeyId:          latest.ko.Spec.KeyID,
			}
			_, err := rm.sdkapi.StopStreamEncryption(ctx, input)
			rm.metrics.RecordAPICall("UPDATE", "StopStreamEncryption", err)
			if err != nil {
				return err
			}
		}
	}

	// if latestEncryptionType is not kms and desiredEncryptionType is kms, we need to start the encryption
	if isEncryptionDisabled(latestEncryptionType) {
		if isEncryptionEnabled(desiredEncryptionType) {
			input := &svcsdk.StartStreamEncryptionInput{
				StreamName:     latest.ko.Spec.Name,
				EncryptionType: types.EncryptionTypeKms,
				KeyId:          desired.ko.Spec.KeyID,
			}
			_, err := rm.sdkapi.StartStreamEncryption(ctx, input)
			rm.metrics.RecordAPICall("UPDATE", "StartStreamEncryption", err)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
