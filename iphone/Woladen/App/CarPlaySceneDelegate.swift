import UIKit
import CarPlay

final class CarPlaySceneDelegate: UIResponder, CPTemplateApplicationSceneDelegate {
    func templateApplicationScene(
        _ templateApplicationScene: CPTemplateApplicationScene,
        didConnect interfaceController: CPInterfaceController,
        to window: CPWindow
    ) {
        interfaceController.setRootTemplate(
            makeRootTemplate(chargerItems: carPlayChargerItems()),
            animated: false,
            completion: nil
        )
    }

    func templateApplicationScene(
        _ templateApplicationScene: CPTemplateApplicationScene,
        didDisconnectInterfaceController interfaceController: CPInterfaceController,
        from window: CPWindow
    ) {
        // no-op scaffold
    }

    private func makeRootTemplate(chargerItems: [CPListItem]) -> CPTemplate {
        let intro = CPListItem(
            text: "Woladen CarPlay",
            detailText: "Scaffold aktiv. Für produktiven Betrieb ist CarPlay-Entitlement + Apple-Kategorie-Freigabe erforderlich."
        )

        let introSection = CPListSection(items: [intro], header: "Status", sectionIndexTitle: nil)
        let chargersSection = CPListSection(
            items: chargerItems,
            header: String(localized: "station.chargingStation"),
            sectionIndexTitle: nil
        )

        return CPListTemplate(title: "Woladen", sections: [introSection, chargersSection])
    }

    private func carPlayChargerItems() -> [CPListItem] {
        [
            CPListItem(
                text: String(localized: "location.deniedTitle"),
                detailText: String(localized: "location.settingsMessage")
            )
        ]
    }
}
