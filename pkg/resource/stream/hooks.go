// Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package stream

import (
	"context"
	"fmt"
	"strings"

	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	util "github.com/aws-controllers-k8s/kinesis-controller/pkg/resource/tags"
)

func isStreamActive(status *string) bool {
	if status == nil {
		return false
	}

	return *status == string(svcsdktypes.StreamStatusActive)
}

func (rm *resourceManager) getTags(ctx context.Context, streamName *string) (map[string]*string, error) {
	return util.GetResourceTags(ctx, rm.sdkapi, rm.metrics, streamName)
}

func (rm *resourceManager) syncTags(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	return util.SyncResourceTags(ctx, rm.sdkapi, rm.metrics, latest.ko.Spec.Name, desired.ko.Spec.Tags, latest.ko.Spec.Tags)
}

// syncRetentionPeriod brings the stream's retention period in line with the
// desired Spec.DesiredRetentionPeriodHours value. The Kinesis API exposes two
// directional operations, so we pick Increase or Decrease based on the observed
// retention period.
func (rm *resourceManager) syncRetentionPeriod(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncRetentionPeriod")
	defer func() { exit(err) }()

	if desired.ko.Spec.DesiredRetentionPeriodHours == nil {
		return nil
	}
	desiredHours := int32(*desired.ko.Spec.DesiredRetentionPeriodHours)
	var observedHours *int32
	if latest.ko.Status.RetentionPeriodHours != nil {
		v := int32(*latest.ko.Status.RetentionPeriodHours)
		observedHours = &v
	}

	if observedHours != nil && desiredHours > *observedHours {
		_, err = rm.sdkapi.IncreaseStreamRetentionPeriod(
			ctx,
			&svcsdk.IncreaseStreamRetentionPeriodInput{
				StreamName:           latest.ko.Spec.Name,
				RetentionPeriodHours: &desiredHours,
			},
		)
		rm.metrics.RecordAPICall("UPDATE", "IncreaseStreamRetentionPeriod", err)
		return err
	}
	if observedHours == nil || desiredHours < *observedHours {
		_, err = rm.sdkapi.DecreaseStreamRetentionPeriod(
			ctx,
			&svcsdk.DecreaseStreamRetentionPeriodInput{
				StreamName:           latest.ko.Spec.Name,
				RetentionPeriodHours: &desiredHours,
			},
		)
		rm.metrics.RecordAPICall("UPDATE", "DecreaseStreamRetentionPeriod", err)
		return err
	}
	return nil
}

// syncEncryption brings the stream's server-side encryption in line with the
// desired Spec.DesiredEncryptionType and Spec.EncryptionKeyARN values. KMS
// encryption requires a customer-managed key ARN; aliases are rejected so that
// the observed Status.KeyID matches the desired value and the controller does
// not re-encrypt on every reconcile.
func (rm *resourceManager) syncEncryption(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncEncryption")
	defer func() { exit(err) }()

	if desired.ko.Spec.DesiredEncryptionType != nil &&
		*desired.ko.Spec.DesiredEncryptionType == string(svcsdktypes.EncryptionTypeKms) {
		if desired.ko.Spec.EncryptionKeyARN == nil {
			return ackerr.NewTerminalError(fmt.Errorf(
				"encryptionKeyARN is required when desiredEncryptionType is KMS"))
		}
		if !strings.HasPrefix(*desired.ko.Spec.EncryptionKeyARN, "arn:") {
			return ackerr.NewTerminalError(fmt.Errorf(
				"encryptionKeyARN must be a KMS key ARN, not an alias or key ID: %q",
				*desired.ko.Spec.EncryptionKeyARN))
		}
		_, err = rm.sdkapi.StartStreamEncryption(
			ctx,
			&svcsdk.StartStreamEncryptionInput{
				StreamName:     latest.ko.Spec.Name,
				EncryptionType: svcsdktypes.EncryptionTypeKms,
				KeyId:          desired.ko.Spec.EncryptionKeyARN,
			},
		)
		rm.metrics.RecordAPICall("UPDATE", "StartStreamEncryption", err)
		return err
	}

	// Desired encryption is NONE (or unset). Stop encryption only if the stream
	// is currently encrypted with KMS.
	if latest.ko.Status.EncryptionType != nil &&
		*latest.ko.Status.EncryptionType == string(svcsdktypes.EncryptionTypeKms) {
		_, err = rm.sdkapi.StopStreamEncryption(
			ctx,
			&svcsdk.StopStreamEncryptionInput{
				StreamName:     latest.ko.Spec.Name,
				EncryptionType: svcsdktypes.EncryptionTypeKms,
				KeyId:          latest.ko.Status.KeyID,
			},
		)
		rm.metrics.RecordAPICall("UPDATE", "StopStreamEncryption", err)
		return err
	}
	return nil
}
