import UIKit
import CarPlay

final class CarPlaySceneDelegate: UIResponder, CPTemplateApplicationSceneDelegate {
    private let repository = ChargerRepository()

    func templateApplicationScene(
        _ templateApplicationScene: CPTemplateApplicationScene,
        didConnect interfaceController: CPInterfaceController,
        to window: CPWindow
    ) {
        interfaceController.setRootTemplate(makeRootTemplate(), animated: false, completion: nil)
    }

    func templateApplicationScene(
        _ templateApplicationScene: CPTemplateApplicationScene,
        didDisconnectInterfaceController interfaceController: CPInterfaceController,
        from window: CPWindow
    ) {
        // no-op scaffold
    }

    private func makeRootTemplate() -> CPTemplate {
        let intro = CPListItem(
            text: "Woladen CarPlay",
            detailText: "Scaffold aktiv. Fuer produktiven Betrieb ist CarPlay-Entitlement + Apple-Kategorie-Freigabe erforderlich."
        )

        let introSection = CPListSection(items: [intro], header: "Status", sectionIndexTitle: nil)
        let chargersSection = CPListSection(
            items: carPlayChargerItems(),
            header: "Schnelllader (offline)",
            sectionIndexTitle: nil
        )

        return CPListTemplate(title: "Woladen", sections: [introSection, chargersSection])
    }

    private func carPlayChargerItems() -> [CPListItem] {
        guard let payload = try? repository.loadData() else {
            return [CPListItem(text: "Keine Daten verfuegbar", detailText: nil)]
        }

        return payload.features.prefix(12).map { feature in
            let p = feature.properties
            let subtitle = "\(p.city) • \(Int(p.displayedMaxPowerKW.rounded())) kW • \(p.chargingPointsCount) Ladepunkte"
            let item = CPListItem(text: p.operatorName, detailText: subtitle)
            item.handler = { _, completion in
                completion()
            }
            return item
        }
    }
}
