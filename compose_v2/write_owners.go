package compose_v2

import "fmt"

// recordMemberWrites 登记已成功成员的写入归属。同一成员可以有序地多次写入同一个 channel。
// owners 包括同一超步此前已经提交的成员，顺序和扇出路径使用相同判据。
func (cm *ChannelManager) recordMemberWrites(owners map[string]FrontierMember, member FrontierMember, writes []ChannelWrite) (map[string]FrontierMember, error) {
	for _, w := range writes {
		if w == nil {
			continue
		}
		name := w.channelName()
		if !cm.isSingleWriterChannel(name) {
			continue
		}
		if prev, ok := owners[name]; ok && prev != member {
			return owners, fmt.Errorf("channel %q 是单写入者（LastValue），却被同一超步的两个成员写了：%s 与 %s。"+
				"改用能合并的 channel，或让同一超步只有一个成员写入: %w",
				name, formatFrontierMember(prev), formatFrontierMember(member), ErrRunAborted)
		}
		if owners == nil {
			owners = make(map[string]FrontierMember)
		}
		owners[name] = member
	}
	return owners, nil
}

// checkPayloadWriteOwners 核对同一逻辑超步的共享 payload。单条指令可以按顺序写同一 channel，
// 也可以改写发送者自己写入的值。独立指令同时写同一个单写入者 channel 时终止运行。
func (cm *ChannelManager) checkPayloadWriteOwners(owners map[string]FrontierMember, sends []SendDirective) error {
	var payloadOwners map[string]int
	for i, d := range sends {
		if d.Target == "" || d.Target == EndNode {
			continue
		}
		owner := FrontierMember{NodeID: d.owner, InstanceID: d.ownerInstanceID}
		for _, w := range d.Payload {
			if w == nil {
				continue
			}
			name := w.channelName()
			if !cm.isSingleWriterChannel(name) {
				continue
			}
			if member, ok := owners[name]; ok && member != owner {
				return fmt.Errorf("channel %q 是单写入者（LastValue），本超步成员 %s 写了它，第 %d 条 SendFrom "+
					"指令（%s → %s）的 payload 也写了它: %w",
					name, formatFrontierMember(member), i, formatFrontierMember(owner), d.Target, ErrRunAborted)
			}
			if prev, ok := payloadOwners[name]; ok && prev != i {
				return fmt.Errorf("channel %q 是单写入者（LastValue），却被同一超步的两条 SendFrom 指令的 payload 写了"+
					"（第 %d 条 → %s、第 %d 条 → %s）。独立入参使用 SendDirective.Input: %w",
					name, prev, sends[prev].Target, i, d.Target, ErrRunAborted)
			}
			if payloadOwners == nil {
				payloadOwners = make(map[string]int)
			}
			payloadOwners[name] = i
		}
	}
	return nil
}
